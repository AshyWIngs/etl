# file: scripts/codes_history_etl.py
# -*- coding: utf-8 -*-
"""
Incremental ETL: Phoenix(HBase) → ClickHouse (native 9000, с компрессией).

Цели: простота, предсказуемость, высокая производительность и отказоустойчивость.

Ключевое поведение
------------------
• Поля переносим «как есть» (без вычислений id/md5, без sys-атрибутов).
• Дедуп на публикации по ключу (c, t, opd) через argMax(..., ingested_at).
• Окно запроса в Phoenix совпадает с бизнес-окном слайса; при желании можно добавить
  «захлёст» назад PHX_QUERY_OVERLAP_MINUTES только на первом слайсе.
  Правую границу не «замораживаем».
• DateTime64(3) передаём как python datetime (naive), микросекунды обрезаем до миллисекунд.
• Столбец `ingested_at` в ClickHouse не передаём в INSERT — он заполняется `DEFAULT now('UTC')`.
  Это устраняет несоответствие числа колонок и не влияет на дедуп (агрегация идёт по `ingested_at` в RAW).
• Никаких CSV — сразу INSERT в ClickHouse (native, compression).

Каденс публикаций
-----------------
• Публикация управляется ТОЛЬКО числом успешных слайсов: PUBLISH_EVERY_SLICES.
• В конце запуска всегда выполняется финальная публикация (ALWAYS_PUBLISH_AT_END зафиксирован).
• Для защиты от «пустых» публикаций действует лёгкий гейтинг: считаем «новые» строки в памяти
  по каждой партиции в рамках текущего запуска и публикуем партицию только если новых строк
  ≥ PUBLISH_MIN_NEW_ROWS (когда PUBLISH_ONLY_IF_NEW=1). В manual-режиме гейтинг отключён.

Как это работает
----------------
1) Разбиваем окно [--since, --until) на слайды по STEP_MIN минут (UTC-времена формируются из CLI; наивные значения трактуются в BUSINESS_TZ (если задана), иначе — в UTC).
2) Для каждого слайда:
   – фиксируем planned→running в журнале;
   – читаем Phoenix батчами (PHX_FETCHMANY_SIZE), сортировка по времени;
   – нормализуем типы и буферизуем; при достижении CH_INSERT_BATCH — INSERT VALUES в RAW.
3) Накапливаем список партиций (toYYYYMMDD(opd) по UTC), задетых слайсом.
4) По достижении порога PUBLISH_EVERY_SLICES выполняем публикацию каждой партиции:
   DROP BUF → INSERT BUF (GROUP BY ... argMax) → REPLACE CLEAN → DROP BUF.
   Всё замеряется и логируется; создаётся запись в журнале `<PROCESS_NAME>:dedup`.
5) По завершении окна выполняем финальную публикацию (если включена).

Надёжность
----------
• Один активный инстанс (advisory-lock) + очистка битых planned/running.
• Мягкий retry CH при UnexpectedPacketFromServerError: reconnect() + повтор.
• Ранний контроль наличия требуемых таблиц ClickHouse (EXISTS TABLE).
• Понятные стартовые ошибки с привязкой к окну запуска в журнале.

CLI (минимальный)
------------------
--since / --until      — окно обработки от и до. --since можно опустить: будет взят watermark из inc_processing_state; --until можно опустить: тогда until = since + RUN_WINDOW_HOURS (по умолчанию 24 ч).
--manual-start         — ручной запуск (журнал: manual_<PROCESS_NAME>).
                         В manual-режиме дополнительно:
                         • публикация выполняется всегда (гейтинг по «новизне» отключён),
                         • в начале запуска выполняется лёгкий backfill пропущенных дней за окно запуска.
--log-level            — уровень логирования (по умолчанию INFO).
"""

# NB: Адаптивная нарезка окна и бэк-офф теперь полностью реализованы в PhoenixClient.fetch_increment_adaptive.
from __future__ import annotations

import argparse
import logging
import os
import json
import socket
import signal
from collections import defaultdict
from threading import Event
try:
    from psycopg.errors import UniqueViolation  # type: ignore[reportMissingImports]
except Exception:  # pragma: no cover - editor/type-checker fallback
    class UniqueViolation(Exception):  # type: ignore[misc]
        """Запасной класс-стаб для psycopg UniqueViolation, когда типовые заглушки недоступны в редакторе."""
        pass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Iterable, Optional, Set, Callable, cast, TYPE_CHECKING
from zoneinfo import ZoneInfo
from dataclasses import dataclass

# Псевдо-декларация для анализатора типов: имя _publish_parts существует ниже.
# Это устраняет ложные "is not defined" для ссылок внутри вложенных хелперов.
if TYPE_CHECKING:
    def _publish_parts(
        ch: "CHClient",
        cfg: "Settings",
        pg: "PGClient",
        process_name: str,
        parts: Set[int],
    ) -> bool: ...

from .logging_setup import setup_logging
from .config import Settings
from .slicer import iter_slices
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient, PGConnectionError
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal
from time import perf_counter
from functools import lru_cache
from urllib.parse import urlparse

log = logging.getLogger("codes_history_increment")

# --- Пакет параметров выполнения для снижения количества аргументов (S107) ---
@dataclass(frozen=True)
class ExecParams:
    """Набор неизменяемых параметров выполнения основного цикла.
    Один объект вместо 18+ параметров снижает шум в сигнатурах и когнитивную
    сложность, не влияя на производительность (обычный доступ к атрибутам).
    """
    journal: "ProcessJournal"
    cfg: "Settings"
    ch: "CHClient"
    phx: "PhoenixClient"
    pg: "PGClient"
    process_name: str
    host: str
    pid: int
    since_dt: datetime
    until_dt: datetime
    step_min: int
    business_tz_name: str
    overlap_delta: timedelta
    publish_every_slices: int
    publish_only_if_new: bool
    publish_min_new_rows: int
    always_publish_at_end: bool
    backfill_missing_enabled: bool


# --- Компактный помощник промежуточной публикации ---
def _maybe_intermediate_publish(
    *,
    pending_parts: Set[int],
    new_rows_by_part: Dict[int, int],
    publish_every_slices: int,
    slices_since_last_pub: int,
    publish_only_if_new: bool,
    publish_min_new_rows: int,
    ch: "CHClient",
    cfg: "Settings",
    pg: "PGClient",
    process_name: str,
    after_publish: Callable[[Set[int]], None],
) -> bool:
    """Промежуточная публикация, если пришло время (вызывается из горячего пути).
    Возвращает True, если публикация состоялась. Внутри — минимальная логика и
    только один вызов `_publish_parts`.
    """
    if publish_every_slices <= 0 or slices_since_last_pub < publish_every_slices:
        return False

    _log_gating_debug(
        pending_parts=pending_parts,
        new_rows_by_part=new_rows_by_part,
        publish_min_new_rows=publish_min_new_rows,
        slices_since_last_pub=slices_since_last_pub,
        publish_every_slices=publish_every_slices,
    )

    parts_to_publish = _select_parts_to_publish(
        pending_parts=pending_parts,
        publish_only_if_new=publish_only_if_new,
        publish_min_new_rows=publish_min_new_rows,
        new_rows_by_part=new_rows_by_part,
    )
    if not parts_to_publish:
        log.info(
            "Промежуточная публикация отложена гейтингом (порог=%d, слайсы=%d/%d).",
            publish_min_new_rows, slices_since_last_pub, publish_every_slices,
        )
        return False

    if _publish_parts(ch, cfg, pg, process_name, parts_to_publish):
        log.info("Промежуточная публикация: %d партиций.", len(parts_to_publish))
        after_publish(parts_to_publish)
        return True
    return False

# Конфигурация времени (ENV/Settings):
# • BUSINESS_TZ — строка таймзоны (IANA, напр. "Asia/Almaty"). Если пустая → наивные даты считаются UTC.
# • INPUT_TZ — БОЛЬШЕ НЕ ИСПОЛЬЗУЕМ; режим выводится из BUSINESS_TZ (см. логику в main()).

# --- Централизованные помощники для логирования ошибок и контроля трейсбека ---
def _want_trace(cfg: Optional["Settings"] = None) -> bool:
    """
    Возвращает True, если нужно печатать traceback.
    Правила:
    • Если в конфиге ETL_TRACE_EXC=1 — печатаем.
    • Если уровень логгера DEBUG — печатаем.
    • Если cfg ещё не создан, смотрим переменную окружения ETL_TRACE_EXC.
    """
    try:
        if cfg is not None and bool(getattr(cfg, "ETL_TRACE_EXC", False)):
            return True
    except Exception:
        pass
    env_flag = os.getenv("ETL_TRACE_EXC", "0").lower() in ("1", "true", "yes", "on")
    return env_flag or logging.getLogger().isEnabledFor(logging.DEBUG)

def _log_maybe_trace(level: int, msg: str, *, exc: Optional[BaseException] = None, cfg: Optional["Settings"] = None) -> None:
    """
    Единый помощник для логирования ошибок: с трейсбеком или без.
    Добавляет полезную подсказку, если стек скрыт.
    """
    if exc is not None and _want_trace(cfg):
        log.log(level, msg, exc_info=True)
    else:
        if exc is not None:
            msg = msg + " (стек скрыт; установите ETL_TRACE_EXC=1 или запустите с --log-level=DEBUG, чтобы напечатать трейсбек)"
        log.log(level, msg)


# --- Вспомогательный метод для снижения шума логов при INFO ---
def _reduce_noise_for_info_mode() -> None:
    """При уровне INFO приглушаем «болтливые» под-логгеры, чтобы логи были чище.
    Ничего не меняем, если включён DEBUG (для полноценной диагностики).
    """
    try:
        # Смотрим именно на наш основной логгер, а не на root
        main_logger = logging.getLogger("codes_history_increment")
        if main_logger.isEnabledFor(logging.DEBUG):
            return
        # В INFO режиме прячем подробные SQL и частые статусы планировщика
        for name in (
            "scripts.db.phoenix_client",   # Phoenix SQL и параметры
            "phoenixdb",                   # внутренние сообщения драйвера
            "urllib3.connectionpool",      # лишние HTTP-диагностики (если PQS/HTTP)
            "scripts.journal",             # planned→running и т.п.
        ):
            try:
                logging.getLogger(name).setLevel(logging.WARNING)
            except Exception:
                pass
    except Exception:
        # Не даём диагностике сломать запуск
        pass


# Грейсфул-остановка по сигналам ОС (SIGTERM/SIGINT/...)
shutdown_event = Event()
_shutdown_reason = ""

def _install_signal_handlers() -> None:
    """Регистрируем обработчики сигналов: выставляют флаг остановки, не кидают исключения сами по себе."""
    def _handle(signum, frame):
        global _shutdown_reason
        try:
            name = signal.Signals(signum).name
        except Exception:
            name = str(signum)
        _shutdown_reason = name
        log.warning("Получен сигнал %s — инициирую мягкую остановку (graceful)...", name)
        shutdown_event.set()

    for _sig in (getattr(signal, "SIGINT", None),
                 getattr(signal, "SIGTERM", None),
                 getattr(signal, "SIGQUIT", None),
                 getattr(signal, "SIGHUP", None)):
        if _sig is not None:
            try:
                signal.signal(_sig, _handle)
            except Exception:
                pass

def _interrupt_message() -> str:
    """Единое текстовое сообщение для журналирования отмены по сигналу/пользователю."""
    if shutdown_event.is_set() and _shutdown_reason != "SIGINT":
        return f"Остановлено сигналом ({_shutdown_reason})"
    return "Остановлено пользователем (SIGINT)"

def _check_stop() -> None:
    if shutdown_event.is_set():
        raise KeyboardInterrupt()

# Порядок колонок соответствует физическим входным полям источника
# (без алиасов/материализованных полей и БЕЗ ingested_at).
# В ClickHouse `ingested_at` выставляется по DEFAULT now('UTC'), поэтому мы его не передаём.
# Tuple вместо list — неизменяемая константа (меньше аллокаций и защита от случайной модификации).
CH_COLUMNS = (
    "c","t","opd",
    "id","did",
    "rid","rinn","rn","sid","sinn","sn","gt","prid",
    "st","ste","elr",
    "emd","apd","exd",
    "p","pt","o","pn","b",
    "tt","tm",
    "ch","j",
    "pg","et",
    "pvad","ag"
)
CH_COLUMNS_STR = ", ".join(CH_COLUMNS)
# Предвычисления для дедупа (ускоряет и упрощает код публикации).
# Агрегируем argMax(..., ingested_at) по данным из RAW.
NON_KEY_COLS = [c for c in CH_COLUMNS if c not in ("c", "t", "opd")]
DEDUP_AGG_EXPRS = ",\n              ".join([f"argMax({c}, ingested_at) AS {c}" for c in NON_KEY_COLS])
DEDUP_SELECT_COLS = "c, t, opd,\n              " + DEDUP_AGG_EXPRS

DT_FIELDS = {"opd","emd","apd","exd","tm"}
# Все целочисленные поля (для быстрого приведения типов без ветвления по разрядности)
INT_FIELDS = {"t","st","ste","elr","pt","et","pg","tt"}

# ------------------------ УТИЛИТЫ ТИПИЗАЦИИ ------------------------

# --- Быстрые чистые хелперы для нормализации времени (минимум ветвлений) ---

def _as_naive_utc_ms(dt: datetime) -> datetime:
    """
    Приводит datetime к naive UTC и обрезает микросекунды до миллисекунд.
    • Aware → переводим в UTC и снимаем tzinfo (phoenix/clickhouse ждут naive UTC).
    • Naive → считаем, что это уже UTC (сохранение поведения), только режем микросекунды до мс.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    # Обрезаем до миллисекунд (CH DateTime64(3))
    if dt.microsecond:
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
    return dt

def _from_epoch_any_seconds_or_ms(value: float) -> datetime:
    """
    Быстрый парсер числового таймстемпа:
    • Принимает секунды или миллисекунды (эвристика: > 10_000_000_000 → делим на 1000).
    • Возвращает naive UTC c точностью до миллисекунд.
    """
    ts = float(value)
    if ts > 10_000_000_000:
        ts = ts / 1000.0
    return _as_naive_utc_ms(datetime.fromtimestamp(ts, tz=timezone.utc))

def _from_string_datetime(s: str) -> Optional[datetime]:
    """
    Быстрый парсер строкового значения времени.
    Порядок:
    1) Пустая строка → None.
    2) Только цифры → epoch (sec/ms).
    3) Суффикс 'Z' → меняем на '+00:00' для fromisoformat.
    4) Пробуем ISO 8601 через datetime.fromisoformat.
    Ошибка парсинга → None (жёстких исключений не кидаем по скорости/устойчивости).
    """
    s = s.strip()
    if not s:
        return None
    if s.isdigit():
        return _from_epoch_any_seconds_or_ms(float(s))
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return _as_naive_utc_ms(datetime.fromisoformat(s))
    except Exception:
        return None

def _to_dt64_obj(v: Any) -> Any:
    """
    Универсальный быстрый нормализатор времени для ClickHouse DateTime64(3):
    • None/"" → None
    • datetime → naive UTC (с обрезкой до миллисекунд)
    • int/float → epoch (sec или ms), naive UTC
    • str:
      – только цифры → epoch (sec/ms)
      – ISO (в т.ч. с 'Z') → парсим через fromisoformat
    Возвращает naive UTC datetime или None. Предпочитаем ранние выходы и «плоские»
    ветвления ради снижения когнитивной сложности и ускорения горячего пути.
    """
    # 1) Самые частые «пустые» значения
    if v is None or v == "":
        return None

    # 2) Уже datetime
    if isinstance(v, datetime):
        return _as_naive_utc_ms(v)

    # 3) Число (секунды/миллисекунды epoch)
    if isinstance(v, (int, float)):
        return _from_epoch_any_seconds_or_ms(float(v))

    # 4) Прочие типы → приводим к строке ровно один раз
    dt = _from_string_datetime(str(v))
    return dt

def _as_int(v: Any) -> Any:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


# --- Helpers for CH storage field parsing ---
def _seq_to_str_list(seq: Iterable[Any]) -> List[str]:
    """Быстрая нормализация произвольной последовательности в список непустых строк (без JSON/regex)."""
    res: List[str] = []
    append = res.append
    for x in seq:
        if x is None:
            continue
        xs = str(x).strip()
        if xs:
            append(xs)
    return res

def _parse_json_list_fast(s: str) -> Optional[List[str]]:
    """
    Узкий быстрый путь: если строка выглядит как JSON-список вида "[...]" — пробуем распарсить.
    При любой ошибке возвращаем None (чтобы упасть в простой split).
    """
    if not (s.startswith("[") and s.endswith("]")):
        return None
    try:
        parsed = json.loads(s)
    except Exception:
        return None
    if not isinstance(parsed, list):
        return None
    return _seq_to_str_list(parsed)

def _split_storage_field_simple(s: str) -> List[str]:
    """
    Универсальный быстрый сплит по распространённым разделителям (без regex) для строкового поля-хранилища.
    • Убираем крайние скобки []{}().
    • Нормализуем разделители к запятой.
    • Режем по запятой и тримим пробелы.
    """
    raw = s.strip("[]{}()")
    if not raw:
        return []
    norm = raw.replace(";", ",").replace("|", ",")
    parts = norm.split(",")
    res: List[str] = []
    append = res.append
    for p in parts:
        p = p.strip()
        if p:
            append(p)
    return res

def _parse_ch_storage(v: Any) -> List[str]:
    """
    Быстрый парсер для поля-хранилища (массив строк в CH).
    Стратегия (минимум ветвлений в «горячем» пути):
    1) Пустые/«псевдопустые» → [].
    2) Уже list/tuple → быстрая нормализация без JSON.
    3) Строка:
       3.1) Если похожа на JSON-список — пытаемся распарсить.
       3.2) Иначе — простой сплит по распространённым разделителям.
    """
    # 1) Пустые/«псевдопустые» — сразу выход
    if v in (None, "", "{}", "[]"):
        return []

    # 2) Уже массив — самый дешёвый путь
    if isinstance(v, (list, tuple)):
        return _seq_to_str_list(v)

    # 3) Приводим к строке один раз
    s = str(v).strip()
    if not s:
        return []

    # 3.1) Узкий быстрый путь: JSON-список вида "[...]"
    parsed = _parse_json_list_fast(s)
    if parsed is not None:
        return parsed

    # 3.2) Простой быстрый сплит
    return _split_storage_field_simple(s)

def _row_to_ch_tuple(r: Dict[str, Any]) -> tuple:
    """Горячий путь: конвертация записи источника в кортеж для INSERT VALUES."""
    dtf = DT_FIELDS
    ints = INT_FIELDS
    out: List[Any] = []
    append = out.append
    for col in CH_COLUMNS:
        val = r.get(col)
        if col in dtf:
            append(_to_dt64_obj(val))
        elif col in ints:
            append(_as_int(val))
        elif col == "ch":
            append(_parse_ch_storage(val))
        else:
            append(None if val == "" else val)
    return tuple(out)

def _opd_to_part_utc(v: Any) -> Optional[int]:
    """
    Быстрое определение партиции (UTC, YYYYMMDD) по значению поля opd.
    Используем тот же быстрый нормализатор времени, что и для вставки в CH.
    Возвращает int YYYYMMDD или None, если значение пустое/непарсибельное.
    """
    dt = _to_dt64_obj(v)
    if not dt:
        return None
    # dt — naive UTC (см. _to_dt64_obj), значит формат YYYYMMDD по UTC корректен:
    return int(dt.strftime("%Y%m%d"))

# ------------------------ ВСПОМОГАТЕЛЬНОЕ ------------------------


def _parts_to_interval(parts: Iterable[int]) -> Tuple[datetime, datetime]:
    ps = sorted(set(parts))
    if not ps:
        now = datetime.now(timezone.utc)
        return now, now
    d0 = datetime.strptime(str(ps[0]), "%Y%m%d").date()
    d1 = datetime.strptime(str(ps[-1]), "%Y%m%d").date()
    start = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
    end   = datetime(d1.year, d1.month, d1.day, tzinfo=timezone.utc) + timedelta(days=1)
    return start, end


# --- Общий быстрый сбор партиций из RAW за окно -------------------------------
# Хелпер вызывается редко (в начале/конце запуска), поэтому вынесен отдельно,
# чтобы не дублировать одинаковый SQL в разных местах. Делает один агрегирующий
# запрос по окну [start_dt; end_dt) и возвращает множество UTC-партиций YYYYMMDD.
from typing import Set as _SetAlias

def _collect_raw_parts_between(ch: "CHClient", table: str, start_dt: datetime, end_dt: datetime) -> _SetAlias[int]:
    try:
        s_str = start_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        e_str = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql = (
            f"SELECT toYYYYMMDD(opd) AS p, count() AS cnt "
            f"FROM {table} "
            f"WHERE opd >= toDateTime('{s_str}') AND opd < toDateTime('{e_str}') "
            f"GROUP BY p"
        )
        rows = ch.execute(sql) or []
        return {int(p) for (p, cnt) in rows if int(cnt) > 0}
    except Exception as ex:
        # Диагностика — best effort: при ошибке не фейлим весь запуск
        log.warning("Backfill: не удалось собрать партиции за окно: %s", ex)
        return set()


# --- Patch: _select_parts_to_publish ---
def _select_parts_to_publish(
    pending_parts: Set[int],
    publish_only_if_new: bool,
    publish_min_new_rows: int,
    new_rows_by_part: Dict[int, int],
) -> Set[int]:
    """
    Чистый хелпер: выбор партиций к публикации.
    • Если гейтинг по «новизне» выключен — публикуем все pending_parts.
    • Иначе — только те, по которым в рамках ТЕКУЩЕГО запуска набежало >= publish_min_new_rows.
    Важное: функция не работает с БД и не логирует — безопасна для вынесения из горячего пути.
    """
    if not pending_parts:
        return set()
    if not publish_only_if_new:
        return set(pending_parts)
    thr = int(publish_min_new_rows)
    return {p for p in pending_parts if int(new_rows_by_part.get(p, 0)) >= thr}

# --- Patch: _log_gating_debug ---
def _log_gating_debug(
    pending_parts: Set[int],
    new_rows_by_part: Dict[int, int],
    publish_min_new_rows: int,
    slices_since_last_pub: int,
    publish_every_slices: int,
) -> None:
    """
    Отладочная печать состояния гейтинга.
    Отдельный чистый хелпер, чтобы не раздувать основной путь.
    Ничего не возвращает.
    """
    if not log.isEnabledFor(logging.DEBUG):
        return
    try:
        dbg_map = {str(p): int(new_rows_by_part.get(p, 0)) for p in sorted(pending_parts)}
        log.debug(
            "Гейтинг: pending_parts=%s; new_rows_by_part=%s; threshold=%d; slices=%d/%d",
            ",".join(str(p) for p in sorted(pending_parts)) or "-",
            json.dumps(dbg_map, ensure_ascii=False),
            publish_min_new_rows, slices_since_last_pub, publish_every_slices,
        )
    except Exception:
        # Логирование — best effort; диагностика не должна мешать работе
        pass

# --- TZ-aware helpers for partition math ---

@lru_cache(maxsize=8)
def _get_tz(tz_name: str) -> ZoneInfo:
    """Кэшируем ZoneInfo по имени (частые вызовы на каждом слайсе)."""
    try:
        return ZoneInfo(tz_name)
    except Exception:
        log.warning("Неизвестная таймзона '%s' — использую UTC", tz_name)
        return ZoneInfo("UTC")


def _iter_partitions_by_day_tz(start_dt: datetime, end_dt: datetime, tz_name: str) -> Iterable[int]:
    if start_dt >= end_dt:
        return []
    tz = _get_tz(tz_name)
    def _as_tz(dt: datetime) -> datetime:
        aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        return aware.astimezone(tz)
    s_loc = _as_tz(start_dt)
    e_loc = _as_tz(end_dt - timedelta(milliseconds=1))
    d = s_loc.date()
    while d <= e_loc.date():
        yield int(d.strftime("%Y%m%d"))
        d += timedelta(days=1)

# ------------------------ РАЗБОР CLI-ДАТ ------------------------

def _parse_cli_dt(value: Optional[str], mode: str, business_tz_name: str) -> Optional[datetime]:
    """
    Разбор CLI-дат: два режима
    - mode='utc'      — наивные строки трактуем как UTC, aware → приводим к UTC.
    - mode='business' — наивные строки трактуем в BUSINESS_TZ (если cfg.BUSINESS_TZ непустой), затем конвертируем в UTC.
    Возвращаем timezone-aware UTC datetime или None.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        raise SystemExit(f"Неверный формат даты/времени: {value!r}")
    if dt.tzinfo is None:
        if mode == "business":
            tz = _get_tz(business_tz_name)
            dt = dt.replace(tzinfo=tz).astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

# ------------------------ PHOENIX FETCH (STRICT) ------------------------

def _iter_phx_batches(phx: PhoenixClient, cfg: Settings, from_utc: datetime, to_utc: datetime):
    """
    Простая совместимая обёртка под нашу реализацию PhoenixClient.fetch_increment:
    fetch_increment(table: str, ts_col: str, columns: List[str], from_dt: datetime, to_dt: datetime)

    ВАЖНО: PQS/phoenixdb ожидает naive UTC datetime, поэтому tzinfo удаляем.
    """
    table = getattr(cfg, "HBASE_MAIN_TABLE", None)
    ts_col = getattr(cfg, "HBASE_MAIN_TS_COLUMN", None)
    if not table or not ts_col:
        raise TypeError("HBASE_MAIN_TABLE/HBASE_MAIN_TS_COLUMN не заданы в конфиге")

    cols: List[str] = [str(c) for c in CH_COLUMNS]

    # Приводим к naive UTC (tzinfo=None)
    f_naive = from_utc.replace(tzinfo=None) if from_utc.tzinfo is not None else from_utc
    t_naive = to_utc.replace(tzinfo=None)   if to_utc.tzinfo   is not None else to_utc

    for batch in phx.fetch_increment(table, ts_col, cols, f_naive, t_naive):
        yield batch

# ------------------------ ОСНОВНОЙ СЦЕНАРИЙ ------------------------

# --- Top-level helper for dedup/publish parts ---
def _publish_parts(
    ch: "CHClient",
    cfg: "Settings",
    pg: "PGClient",
    process_name: str,
    parts: Set[int],
) -> bool:
    """
    Дедуп/публикация набора партиций.
    Схема: DROP BUF → INSERT BUF (GROUP BY c,t,opd с argMax(..., ingested_at)) → REPLACE PARTITION → DROP BUF.
    Возвращает True, если была выполнена хотя бы одна REPLACE.
    """
    if not parts:
        return False

    # Определяем ON CLUSTER (если указано)
    on_cluster = ""
    try:
        cluster_name = getattr(cfg, "CH_CLUSTER", None)
        if cluster_name:
            on_cluster = f" ON CLUSTER {cluster_name}"
    except Exception:
        pass

    published_any = False
    parts_sorted = sorted(set(parts))

    # Имена таблиц берём из cfg
    ch_table_raw_all   = cfg.CH_RAW_TABLE
    ch_table_clean     = cfg.CH_CLEAN_TABLE
    ch_table_buf       = cfg.CH_DEDUP_BUF_TABLE

    # Журнал для этапа дедупа/публикации
    try:
        dedup_journal = ProcessJournal(cast(Any, pg), cfg.JOURNAL_TABLE, f"{process_name}:dedup")  # type: ignore[arg-type]
        dedup_journal.ensure()
    except Exception:
        dedup_journal = None

    pub_start, pub_end = _parts_to_interval(parts_sorted)
    try:
        if dedup_journal:
            dedup_journal.mark_running(pub_start, pub_end, host=socket.gethostname(), pid=os.getpid())
    except Exception:
        pass

    for p in parts_sorted:
        try:
            # 1) Чистим буферную партицию (если была)
            try:
                ch.execute(f"ALTER TABLE {ch_table_buf}{on_cluster} DROP PARTITION {p}")
            except Exception:
                pass

            # 2) Дедупим в буфер: перечисляем только «пользовательские» колонки (без ingested_at)
            insert_sql = (
                f"INSERT INTO {ch_table_buf} ({CH_COLUMNS_STR}) "
                f"SELECT {DEDUP_SELECT_COLS} "
                f"FROM {ch_table_raw_all} "
                f"WHERE toYYYYMMDD(opd) = {p} "
                f"GROUP BY c, t, opd"
            )
            ch.execute(insert_sql)

            # 3) Публикация REPLACE PARTITION из буфера в CLEAN
            replace_sql = f"ALTER TABLE {ch_table_clean}{on_cluster} REPLACE PARTITION {p} FROM {ch_table_buf}"
            ch.execute(replace_sql)
            published_any = True

        except Exception as ex:
            # «Пустая» партиция (0 строк в буфере) — это не фатал
            _log_maybe_trace(logging.WARNING, f"Публикация партиции {p} пропущена: {ex}", exc=ex, cfg=cfg)
        finally:
            # 4) Убираем временную партицию независимо от исхода REPLACE
            try:
                ch.execute(f"ALTER TABLE {ch_table_buf}{on_cluster} DROP PARTITION {p}")
            except Exception:
                pass

    try:
        if dedup_journal:
            dedup_journal.mark_done(pub_start, pub_end, rows_read=0, rows_written=0)
    except Exception:
        pass

    return published_any

# --- Helpers for business time window and TZ context ---
def _resolve_business_tz(cfg: "Settings") -> Tuple[str, str]:
    """
    Возвращает (business_tz_name, input_tz_mode).
    Если BUSINESS_TZ не задан — используем UTC и трактуем наивные даты как UTC.
    """
    business_tz_raw = str(getattr(cfg, "BUSINESS_TZ", "") or "").strip()
    business_tz_name = business_tz_raw or "UTC"
    input_tz_mode = "business" if business_tz_raw else "utc"
    return business_tz_name, input_tz_mode

def _parse_since_arg_or_watermark(
    args: argparse.Namespace,
    journal: "ProcessJournal",
    input_tz_mode: str,
    business_tz_name: str,
) -> Tuple[datetime, bool]:
    """
    Разбирает since: если указан — парсим; иначе берём watermark из журнала.
    Возвращает (since_dt, used_watermark).
    """
    if getattr(args, "since", None):
        since_dt = _parse_cli_dt(args.since, input_tz_mode, business_tz_name)
        if since_dt is None:
            raise SystemExit("Не удалось разобрать --since (проверьте формат даты/времени)")
        return since_dt, False

    # watermark из журнала (UTC)
    wm = getattr(journal, "get_watermark", lambda: None)() or None
    if wm is None:
        raise SystemExit("Не задан --since и нет watermark в журнале")
    return wm, True

def _parse_until_arg_or_default(
    args: argparse.Namespace,
    since_dt: datetime,
    cfg: "Settings",
    input_tz_mode: str,
    business_tz_name: str,
) -> Tuple[datetime, bool, int]:
    """
    Разбирает until: если указан — парсим; иначе берём since + RUN_WINDOW_HOURS (дефолт 24).
    Возвращает (until_dt, auto_until, window_hours).
    """
    if getattr(args, "until", None):
        until_dt = _parse_cli_dt(args.until, input_tz_mode, business_tz_name)
        if until_dt is None:
            raise SystemExit("Не удалось разобрать --until (проверьте формат даты/времени)")
        return until_dt, False, 0  # window_hours не нужен, т.к. не авто

    # Авто-режим: since + RUN_WINDOW_HOURS
    try:
        win_h_raw = getattr(cfg, "RUN_WINDOW_HOURS", None)
        window_hours = int(win_h_raw) if win_h_raw is not None else 24
    except Exception:
        window_hours = 24
    if window_hours <= 0:
        window_hours = 24
    return since_dt + timedelta(hours=window_hours), True, window_hours

def _build_time_window(cfg: "Settings", args: argparse.Namespace, journal: "ProcessJournal") -> Tuple[datetime, datetime, bool, bool, int, str, str]:
    """
    Единая сборка бизнес-окна запуска и TZ-контекста.
    Возвращает кортеж:
      (since_dt, until_dt, used_watermark, auto_until, auto_window_hours, business_tz_name, input_tz_mode)
    """
    # 1) Определяем бизнес‑таймзону и политику трактования наивных дат
    business_tz_name, input_tz_mode = _resolve_business_tz(cfg)

    # 2) Разбираем since (или берём watermark)
    since_dt, used_watermark = _parse_since_arg_or_watermark(args, journal, input_tz_mode, business_tz_name)

    # 3) Разбираем until (или считаем from since + RUN_WINDOW_HOURS)
    until_dt, auto_until, auto_window_hours = _parse_until_arg_or_default(args, since_dt, cfg, input_tz_mode, business_tz_name)

    # 4) Валидация окна
    if until_dt <= since_dt:
        raise SystemExit(f"Некорректное окно: since({since_dt.isoformat()}) >= until({until_dt.isoformat()})")

    return (since_dt, until_dt, used_watermark, auto_until, auto_window_hours, business_tz_name, input_tz_mode)

def _perform_startup_backfill(
    *,
    ch: "CHClient",
    cfg: "Settings",
    until_dt: datetime,
    process_name: str,
    pg: "PGClient",
) -> None:
    """
    Лёгкий бэкфилл «пропущенных» партиций в начале запуска.
    Безопасен и идемпотентен (использует REPLACE PARTITION).
    """
    try:
        backfill_days = int(getattr(cfg, "STARTUP_BACKFILL_DAYS", 0) or 0)
    except Exception:
        backfill_days = 0
    if backfill_days <= 0:
        return

    bf_end = until_dt
    bf_start = bf_end - timedelta(days=backfill_days)

    # Собираем партиции из RAW за окно одним агрегирующим запросом (общий хелпер)
    parts_recent = _collect_raw_parts_between(ch, cfg.CH_RAW_TABLE, bf_start, bf_end)
    if parts_recent:
        log.info("Стартовый бэкфилл: допубликую %d партиций за последние %d дней.", len(parts_recent), backfill_days)
        _publish_parts(ch, cfg, pg, process_name, parts_recent)
    else:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Стартовый бэкфилл: в RAW нет данных за последние %d дней — пропускаю.", backfill_days)

def _finalize_publication(
    *,
    ch: "CHClient",
    cfg: "Settings",
    pg: "PGClient",
    process_name: str,
    pending_parts: Set[int],
    since_dt: datetime,
    until_dt: datetime,
    backfill_missing_enabled: bool,
    collect_missing_parts_between: Callable[[ "CHClient", datetime, datetime], Set[int]],
    new_rows_since_pub_by_part: Dict[int, int],
) -> None:
    """
    Финальная публикация (точка согласования) и опциональный добор пропущенных партиций за окно запуска.
    """
    if backfill_missing_enabled:
        try:
            missing_parts = collect_missing_parts_between(ch, since_dt, until_dt)
            for mp in (missing_parts or []):
                pending_parts.add(mp)
        except Exception as e:
            log.warning("Backfill: сбор пропущенных партиций завершился с ошибкой: %s", e)

    if pending_parts:
        log.info("Финальная публикация: %d партиций.", len(pending_parts))
        _published_now = set(pending_parts)
        _publish_parts(ch, cfg, pg, process_name, _published_now)
        for _p in _published_now:
            new_rows_since_pub_by_part[_p] = 0
            pending_parts.discard(_p)
    else:
        log.info("Финальная публикация: нечего публиковать.")

def _compute_phx_slice_and_log(
    s: datetime,
    e: datetime,
    is_first_slice: bool,
    overlap_delta: timedelta,
    business_tz_name: str,
    logged_tz_context: bool,
) -> Tuple[datetime, datetime, bool]:
    """
    Вычисляет реальные границы окна для Phoenix (с учётом возможного «захлёста» на первом слайсе)
    и печатает разовый контекст TZ + подробный DEBUG-лог по окну.
    Возвращает (s_q, e_q, logged_tz_context).
    """
    # Захлёст (только на первом слайсе, если включён)
    use_overlap = overlap_delta if (is_first_slice and overlap_delta > timedelta(0)) else timedelta(0)
    s_q = s - use_overlap if use_overlap > timedelta(0) else s
    e_q = e  # правую границу не сдвигаем

    # Разовый лог TZ-контекста
    if not logged_tz_context:
        try:
            _s_aware = s if s.tzinfo else s.replace(tzinfo=timezone.utc)
            _biz_offset_min = int(((_s_aware.astimezone(_get_tz(business_tz_name))).utcoffset() or timedelta(0)).total_seconds() // 60)
        except Exception:
            _biz_offset_min = 0
        log.info("TZ context: business_tz=%s, business_offset=%+d мин; partitions=UTC",
                 business_tz_name, _biz_offset_min)
        logged_tz_context = True

    # Подробности окна — только в DEBUG
    if log.isEnabledFor(logging.DEBUG):
        if use_overlap > timedelta(0):
            log.debug(
                "Слайс (бизнес): %s → %s | Phoenix: %s → %s | overlap=%d мин",
                s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
                int(use_overlap.total_seconds() // 60),
            )
        else:
            log.debug(
                "Слайс (бизнес): %s → %s | Phoenix: %s → %s",
                s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
            )
    return s_q, e_q, logged_tz_context


def _plan_run_with_heartbeat(
    journal: "ProcessJournal",
    s: datetime,
    e: datetime,
    host: str,
    pid: int,
    hb_interval_sec: int,
    total_written_ch: int,
) -> Tuple[int, Callable[[int, int], None]]:
    """
    Планирует слайс (с санацией конфликтующих planned), переводит в running и
    возвращает (run_id, maybe_hb).
    `maybe_hb(rows_read, total_written_ch)` — замыкание с троттлингом по времени.
    """
    # Санация «планов», затем mark_planned с защитой от гонки (UniqueViolation)
    try:
        _clear_planned = getattr(journal, "clear_conflicting_planned", None)
        if callable(_clear_planned):
            _clear_planned(s, e)
    except Exception:
        pass
    try:
        journal.mark_planned(s, e)
    except UniqueViolation:
        _clear_planned = getattr(journal, "clear_conflicting_planned", None)
        if callable(_clear_planned):
            _clear_planned(s, e)
        journal.mark_planned(s, e)

    run_id = journal.mark_running(s, e, host=host, pid=pid)

    # Жёсткая нижняя граница, чтобы не «топить» БД
    if hb_interval_sec < 5:
        hb_interval_sec = 5
    _next_deadline = perf_counter() + hb_interval_sec

    # Стартовый heartbeat (best effort)
    try:
        journal.heartbeat(run_id, progress={"rows_read": 0, "rows_written": total_written_ch})
    except Exception:
        pass

    def maybe_hb(rows_read: int, total_written: int) -> None:
        nonlocal _next_deadline
        nowp = perf_counter()
        if nowp >= _next_deadline:
            try:
                journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written})
            except Exception:
                pass
            _next_deadline = nowp + hb_interval_sec

    return run_id, maybe_hb


def _connect_pg_and_journal(cfg: "Settings", process_name: str) -> Tuple["PGClient", "ProcessJournal"]:
    """
    Подключение к Postgres и инициализация журнала запусков.
    Быстрый фаст‑фейл при ошибке, с печатью подробностей (при DEBUG/ETL_TRACE_EXC=1).
    Возвращает кортеж (pg, journal).
    """
    try:
        pg = PGClient(cfg.PG_DSN)
    except PGConnectionError as e:
        _log_maybe_trace(logging.CRITICAL, f"FATAL: postgres connect/init failed: {e}", exc=e, cfg=cfg)
        raise SystemExit(2)
    except Exception as e:
        _log_maybe_trace(logging.CRITICAL, f"FATAL: unexpected error during postgres init: {e.__class__.__name__}: {e}", exc=e, cfg=cfg)
        raise SystemExit(2)

    journal = ProcessJournal(cast(Any, pg), cfg.JOURNAL_TABLE, process_name)  # type: ignore[arg-type]
    journal.ensure()
    # Безопасно создаём/обновляем строку состояния процесса (если поддерживается журналом).
    try:
        if hasattr(journal, "_state_upsert"):
            journal._state_upsert(status="idle", healthy=None, extra={})
    except Exception:
        # Не препятствуем запуску, если bootstrap состояния не удался
        pass
    return pg, journal


def _startup_fail_with_journal(
    journal: "ProcessJournal",
    since_dt: datetime,
    until_dt: datetime,
    cfg: "Settings",
    component: str,
    exc: Exception,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Единый обработчик стартовых ошибок компонентов (PG/CH/Phoenix).
    Пишет подробный лог + фиксирует ошибку в журнале, после чего аварийно завершает процесс.
    """
    msg = f"{component} connect/init failed: {exc}"
    _log_maybe_trace(logging.ERROR, f"Стартовая ошибка компонента {component}: {msg}", exc=exc, cfg=cfg)
    try:
        _mark_startup_error = getattr(journal, "mark_startup_error", None)
        if callable(_mark_startup_error):
            _mark_startup_error(
                msg,
                component=component,
                since=since_dt,
                until=until_dt,
                host=socket.gethostname(),
                pid=os.getpid(),
                extra=extra or {},
            )
    except Exception:
        # Журнал — best effort, не даём вторичной ошибке перекрыть исходную
        pass
    raise SystemExit(2)


def _connect_phx_client(
    cfg: "Settings",
    on_fail: Callable[[str, Exception, Optional[Dict[str, Any]]], None],
) -> "PhoenixClient":
    """
    Подключение к Phoenix(PQS). Все «дорогие» параметры (fetchmany_size и пр.)
    берём из конфига, TCP‑пробу не трогаем (сохраняем переменную PHX_TCP_PROBE_TIMEOUT_MS).
    """
    try:
        client = PhoenixClient(cfg.PQS_URL, fetchmany_size=cfg.PHX_FETCHMANY_SIZE)
    except Exception as e:
        # ВАЖНО: передаём extra позиционно, т.к. on_fail типизирован как Callable[[str, Exception, Optional[Dict[str, Any]]], None]
        # и Pylance ожидает ровно позиционные аргументы (если передать extra=..., получим "Expected 1 more positional argument").
        on_fail("phoenix", e, {"url": cfg.PQS_URL})
        raise  # недостижимо
    # Короткий информативный лог хоста/порта
    try:
        _u = urlparse(cfg.PQS_URL)
        _host = _u.hostname or cfg.PQS_URL
        _port = f":{_u.port}" if _u.port else ""
        log.info("Phoenix(PQS) подключён (%s%s)", _host, _port)
    except Exception:
        log.info("Phoenix(PQS) клиент инициализирован")
    return client


def _connect_ch_client(
    cfg: "Settings",
    on_fail: Callable[[str, Exception, Optional[Dict[str, Any]]], None],
) -> "CHClient":
    """
    Подключение к ClickHouse (native, компрессия включена). В случае ошибки — единый обработчик.
    """
    try:
        ch = CHClient(
            hosts=cfg.ch_hosts_list(),
            port=cfg.CH_PORT,
            database=cfg.CH_DB,
            user=cfg.CH_USER,
            password=cfg.CH_PASSWORD,
            compression=True,
        )
    except Exception as e:
        # ВАЖНО: extra только позиционно — см. комментарий про Callable и проверку Pylance.
        on_fail("clickhouse", e, {"hosts": cfg.ch_hosts_list(), "db": cfg.CH_DB})
        raise  # недостижимо
    try:
        log.info("ClickHouse: db=%s, cluster=%s, hosts=%s", cfg.CH_DB, getattr(cfg, "CH_CLUSTER", None), ",".join(cfg.ch_hosts_list()))
    except Exception:
        pass
    return ch


def _ensure_ch_tables_or_fail(
    ch: "CHClient",
    cfg: "Settings",
    required_tables: List[str],
    on_fail: Callable[[str, Exception, Optional[Dict[str, Any]]], None],
) -> None:
    """
    Ранний контроль наличия критичных таблиц ClickHouse — быстрый «фатальный» выход при проблеме.
    """
    try:
        ch.ensure_tables(
            required_tables,
            db=cfg.CH_DB,
            cluster=getattr(cfg, "CH_CLUSTER", None),
            try_switch_host=True,
        )
    except Exception as ex:
        # ВАЖНО: extra передаём позиционно — иначе Pylance ругается на недостаток позиционных аргументов
        on_fail(
            "clickhouse",
            ex,
            {
                "stage": "ensure_tables",
                "db": cfg.CH_DB,
                "hosts": cfg.ch_hosts_list(),
                "required": required_tables,
            },
        )


def _prepare_publish_flags(cfg: "Settings", manual_mode: bool) -> Tuple[int, bool, bool, int]:
    """
    Нормализация флагов публикации:
    • publish_every_slices — каждые N слайсов делаем публикацию (0 → никогда в середине).
    • always_publish_at_end — финальная публикация всегда включена (точка согласования).
    • publish_only_if_new — гейтинг по «новизне» (в manual-режиме выключен).
    • publish_min_new_rows — порог «новых» строк на партицию.
    """
    publish_every_slices = int(getattr(cfg, "PUBLISH_EVERY_SLICES", 0) or 0)
    always_publish_at_end = True  # фиксируем ожидание: в конце всегда публикуем
    publish_only_if_new = bool(int(getattr(cfg, "PUBLISH_ONLY_IF_NEW", 1))) and (not manual_mode)
    publish_min_new_rows = int(getattr(cfg, "PUBLISH_MIN_NEW_ROWS", 1))
    return publish_every_slices, always_publish_at_end, publish_only_if_new, publish_min_new_rows


# --- Вспомогательные хелперы для конфигов и параметров ETL ---
def _required_ch_tables(cfg: "Settings") -> List[str]:
    """
    Компактный список обязательных таблиц ClickHouse.
    Вынос в отдельный хелпер снижает когнитивную сложность места вызова
    и делает состав таблиц очевиднее.
    """
    return [
        t for t in (
            cfg.CH_RAW_TABLE,
            cfg.CH_CLEAN_TABLE,
            getattr(cfg, "CH_CLEAN_ALL_TABLE", None),
            cfg.CH_DEDUP_BUF_TABLE,
        ) if t
    ]


def _resolve_phx_overlap(cfg: "Settings") -> timedelta:
    """
    Нормализация «захлёста» окна запроса в Phoenix.
    Возвращает timedelta(minutes=PHX_QUERY_OVERLAP_MINUTES) либо 0.
    Вынесено для снижения количества ветвлений в _run_etl_impl.
    """
    try:
        m = int(getattr(cfg, "PHX_QUERY_OVERLAP_MINUTES", 0) or 0)
    except Exception:
        m = 0
    return timedelta(minutes=m) if m > 0 else timedelta(0)


def _resolve_step_min(cfg: "Settings") -> int:
    """
    Нормализация шага слайсера (в минутах). Вынос в хелпер убирает условность
    из _run_etl_impl и делает поведение явным.
    """
    try:
        step = int(getattr(cfg, "STEP_MIN", 60))
    except Exception:
        step = 60
    return step



# --- Мелкие чистые хелперы для снижения когнитивной сложности «горячей» функции ---

def _phx_naive_bounds(s_q: datetime, e_q: datetime) -> Tuple[datetime, datetime]:
    """Переводит aware-границы запроса к Phoenix в naive UTC (phoenixdb этого требует)."""
    s_q_phx = s_q.replace(tzinfo=None) if s_q.tzinfo else s_q
    e_q_phx = e_q.replace(tzinfo=None) if e_q.tzinfo else e_q
    return s_q_phx, e_q_phx


def _resolve_phx_table_and_cols(cfg: "Settings") -> Tuple[str, str, List[str]]:
    """Быстрый и явный резолв таблицы/TS-колонки/набора столбцов для Phoenix.
    В отдельной функции, чтобы не добавлять ветвления в _process_one_slice.
    """
    table = getattr(cfg, "HBASE_MAIN_TABLE", None)
    ts_col = getattr(cfg, "HBASE_MAIN_TS_COLUMN", None)
    if not table or not ts_col:
        raise TypeError("HBASE_MAIN_TABLE/HBASE_MAIN_TS_COLUMN не заданы в конфиге")
    cols: List[str] = [str(c) for c in CH_COLUMNS]
    return cast(str, table), cast(str, ts_col), cols


def _flush_ch_buffer(
    *,
    ch_table_raw_all: str,
    ch_rows_local: List[tuple],
    insert_rows: Callable[[str, List[tuple], Tuple[str, ...]], int],
    maybe_hb: Callable[[int, int], None],
    rows_read_snapshot: int,
) -> int:
    """Финальный/пороговый сброс буфера в ClickHouse. Возвращает число записанных строк.
    Вынесено в отдельный хелпер ради читаемости и -1 к когнитивной сложности.
    """
    if not ch_rows_local:
        return 0
    written = insert_rows(ch_table_raw_all, ch_rows_local, CH_COLUMNS)
    ch_rows_local.clear()
    # Heartbeat после фактической записи — best effort
    maybe_hb(rows_read_snapshot, written)
    return written


def _proc_phx_batch(
    batch: Iterable[Dict[str, Any]],
    *,
    ch_batch: int,
    ch_rows_local: List[tuple],
    row_to_tuple: Callable[[Dict[str, Any]], tuple],
    opd_to_part: Callable[[Any], Optional[int]],
    new_rows_since_pub_by_part: Dict[int, int],
    insert_rows: Callable[[str, List[tuple], Tuple[str, ...]], int],
    ch_table_raw_all: str,
    maybe_hb: Callable[[int, int], None],
    rows_read_snapshot: int,
) -> int:
    """Обработка одного batсh из Phoenix: подготовка кортежей → пороговые INSERT'ы в CH.
    Возвращает, сколько строк записано в CH в рамках обработки конкретного batch.
    Выделено отдельно, чтобы убрать вложенные ветвления из _process_one_slice.
    """
    written_now = 0
    append_row = ch_rows_local.append
    for r in batch:
        p = opd_to_part(r.get("opd"))
        if p is not None:
            new_rows_since_pub_by_part[p] += 1
        append_row(row_to_tuple(r))
        # Пороговая отправка в ClickHouse — минимальные проверки в «горячем пути»
        if ch_batch > 0 and len(ch_rows_local) >= ch_batch:
            written_now += insert_rows(ch_table_raw_all, ch_rows_local, CH_COLUMNS)
            ch_rows_local.clear()
            maybe_hb(rows_read_snapshot, written_now)
    return written_now


# --- Вынос «горячей» логики одного слайса в отдельный быстрый хелпер ---
# Цель: снизить когнитивную сложность _execute_with_lock, не трогая поведение.
# Здесь нет журналирования статусов (кроме heartbeat) и публикации —
# только чтение из Phoenix → нормализация → batch INSERT в RAW + сбор партиций.

def _process_one_slice(
    *,
    cfg: "Settings",
    phx: "PhoenixClient",
    ch_table_raw_all: str,
    ch_batch: int,
    s_q: datetime,
    e_q: datetime,
    row_to_tuple: Callable[[Dict[str, Any]], tuple],
    opd_to_part: Callable[[Any], Optional[int]],
    insert_rows: Callable[[str, List[tuple], Tuple[str, ...]], int],
    maybe_hb: Callable[[int, int], None],
    new_rows_since_pub_by_part: Dict[int, int],
    pending_parts: Set[int],
) -> Tuple[int, int]:
    """
    Обрабатывает один слайс окна без побочных эффектов журналирования и публикации.
    Возвращает: (rows_read, rows_written_now).

    Скорость/стабильность:
    • Убраны неиспользуемые параметры — минус аллокации и предупреждения анализаторов.
    • Вспомогательные хелперы (_phx_naive_bounds/_resolve_phx_table_and_cols/_proc_phx_batch/_flush_ch_buffer)
      выносят ветвления и уменьшают когнитивную сложность без влияния на «горячий путь».
    """
    # 1) Границы запроса для Phoenix в naive UTC (phoenixdb этого требует)
    s_q_phx, e_q_phx = _phx_naive_bounds(s_q, e_q)

    # 2) Параметры таблицы источника
    table, ts_col, cols = _resolve_phx_table_and_cols(cfg)

    rows_read = 0
    written_now = 0
    ch_rows_local: List[tuple] = []

    # 3) Чтение данных из Phoenix с адаптивной вытяжкой
    for batch in phx.fetch_increment_adaptive(table, ts_col, cols, s_q_phx, e_q_phx):
        # NB: избегаем лишних ветвлений — простая проверка размера
        batch_len = len(batch) if batch else 0
        if batch_len:
            rows_read += batch_len
            written_now += _proc_phx_batch(
                batch,
                ch_batch=ch_batch,
                ch_rows_local=ch_rows_local,
                row_to_tuple=row_to_tuple,
                opd_to_part=opd_to_part,
                new_rows_since_pub_by_part=new_rows_since_pub_by_part,
                insert_rows=insert_rows,
                ch_table_raw_all=ch_table_raw_all,
                maybe_hb=maybe_hb,
                rows_read_snapshot=rows_read,
            )
        # Heartbeat по времени/объёму — независимо от пустых батчей
        maybe_hb(rows_read, written_now)

    # 4) Финальный доброс в CH (если что-то осталось в буфере)
    written_now += _flush_ch_buffer(
        ch_table_raw_all=ch_table_raw_all,
        ch_rows_local=ch_rows_local,
        insert_rows=insert_rows,
        maybe_hb=maybe_hb,
        rows_read_snapshot=rows_read,
    )

    # 5) Партиции UTC, затронутые слайсом (используем уже готовый быстрый генератор)
    for p in _iter_partitions_by_day_tz(s_q, e_q, "UTC"):
        pending_parts.add(p)

    return rows_read, written_now

def _run_one_slice_and_maybe_publish(
    params: ExecParams,
    s: datetime,
    e: datetime,
    *,
    ch_table_raw_all: str,
    ch_batch: int,
    logged_tz_context: bool,
    is_first_slice: bool,
    slices_since_last_pub: int,
    total_written_ch: int,
    pending_parts: Set[int],
    new_rows_since_pub_by_part: Dict[int, int],
    after_publish: Callable[[Set[int]], None],
) -> Tuple[int, int, bool, bool, int, int]:
    """
    Выполняет полный цикл обработки одного слайса: планирование в журнале, чтение Phoenix,
    batch‑вставки в RAW, промежуточная публикация и финальный mark_done.
    Возвращает:
        rows_read, written_now, is_first_slice, logged_tz_context, slices_since_last_pub, total_written_ch
    """
    # 1) Окно Phoenix + разовый лог TZ-контекста
    s_q, e_q, logged_tz_context = _compute_phx_slice_and_log(
        s,
        e,
        is_first_slice,
        params.overlap_delta,
        params.business_tz_name,
        logged_tz_context,
    )

    # 2) Планирование слайса + heartbeat с троттлингом
    hb_interval_sec = int(getattr(params.cfg, "JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", 300) or 300)
    run_id, maybe_hb = _plan_run_with_heartbeat(
        params.journal,
        s,
        e,
        params.host,
        params.pid,
        hb_interval_sec,
        total_written_ch,
    )

    # 3) Основная работа: чтение Phoenix → нормализация → INSERT в CH
    try:
        rows_read, written_now = _process_one_slice(
            cfg=params.cfg,
            phx=params.phx,
            ch_table_raw_all=ch_table_raw_all,
            ch_batch=ch_batch,
            s_q=s_q,
            e_q=e_q,
            row_to_tuple=_row_to_ch_tuple,
            opd_to_part=_opd_to_part_utc,
            insert_rows=params.ch.insert_rows,
            maybe_hb=maybe_hb,
            new_rows_since_pub_by_part=new_rows_since_pub_by_part,
            pending_parts=pending_parts,
        )
        total_written_ch = total_written_ch + written_now
        slices_since_last_pub += 1
        is_first_slice = False

        # 4) Промежуточная публикация ДО mark_done()
        _maybe_intermediate_publish(
            pending_parts=pending_parts,
            new_rows_by_part=new_rows_since_pub_by_part,
            publish_every_slices=params.publish_every_slices,
            slices_since_last_pub=slices_since_last_pub,
            publish_only_if_new=params.publish_only_if_new,
            publish_min_new_rows=params.publish_min_new_rows,
            ch=params.ch,
            cfg=params.cfg,
            pg=params.pg,
            process_name=params.process_name,
            after_publish=after_publish,
        )

        # 5) Закрываем слайс в журнале и печатаем компактный итог
        params.journal.mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
        _pp = ",".join(str(p) for p in sorted(pending_parts)) or "-"
        log.info(
            "Слайс завершён: %s → %s (PHX: %s → %s); rows_read=%d, rows_written_raw_total=%d, pending_parts=%s",
            s.isoformat(),
            e.isoformat(),
            s_q.isoformat(),
            e_q.isoformat(),
            rows_read,
            total_written_ch,
            _pp,
        )
        return rows_read, written_now, is_first_slice, logged_tz_context, slices_since_last_pub, total_written_ch

    except KeyboardInterrupt:
        try:
            _mark_cancelled = getattr(params.journal, "mark_cancelled", None)
            if callable(_mark_cancelled):
                _mark_cancelled(s, e, message=_interrupt_message())
            else:
                params.journal.mark_error(s, e, message=_interrupt_message())
        except Exception:
            pass
        raise
    except Exception as ex:
        # NB: фиксируем прогресс и ошибку; пробрасываем исключение наверх — поведение не меняется.
        params.journal.heartbeat(run_id, progress={"error": str(ex)})
        params.journal.mark_error(s, e, message=str(ex))
        raise

def _execute_with_lock(params: ExecParams) -> Tuple[int, int]:
    """
    Основной сценарий выполнения под advisory‑lock из журнала.
    Возвращает (total_read, total_written_ch).
    ВНИМАНИЕ: параметры сгруппированы в ExecParams для снижения S107; внутри
    активно используются локальные ссылки на поля params для скорости.
    """
    # Локальные ссылки (ускоряют доступ в горячем пути)
    journal = params.journal
    cfg = params.cfg
    ch = params.ch
    pg = params.pg
    process_name = params.process_name
    since_dt = params.since_dt
    until_dt = params.until_dt
    step_min = params.step_min
    always_publish_at_end = params.always_publish_at_end
    backfill_missing_enabled = params.backfill_missing_enabled

    total_read = 0
    total_written_ch = 0
    pending_parts: Set[int] = set()
    slices_since_last_pub = 0
    new_rows_since_pub_by_part: Dict[int, int] = defaultdict(int)

    ch_table_raw_all = cfg.CH_RAW_TABLE
    ch_batch = int(cfg.CH_INSERT_BATCH)

    def _after_publish_update_state(published: Set[int]) -> None:
        nonlocal slices_since_last_pub
        for p in published:
            new_rows_since_pub_by_part[p] = 0
            pending_parts.discard(p)
        slices_since_last_pub = 0

    try:
        with journal.exclusive_lock() as got:
            if not got:
                log.warning("Другой инстанс '%s' уже выполняется — выходим.", process_name)
                return total_read, total_written_ch

            # Санация «зависших» запусков
            journal.sanitize_stale(
                planned_ttl_minutes=60,
                running_heartbeat_timeout_minutes=45,
                running_hard_ttl_hours=12,
            )

            # Лёгкий стартовый бэкфилл недопубликованных партиций (REPLACE идемпотентен)
            _perform_startup_backfill(ch=ch, cfg=cfg, until_dt=until_dt, process_name=process_name, pg=pg)

            logged_tz_context = False
            is_first_slice = True

            for s, e in iter_slices(since_dt, until_dt, step_min):
                _check_stop()
                rows_read, _, is_first_slice, logged_tz_context, slices_since_last_pub, total_written_ch = _run_one_slice_and_maybe_publish(
                    params,
                    s,
                    e,
                    ch_table_raw_all=ch_table_raw_all,
                    ch_batch=ch_batch,
                    logged_tz_context=logged_tz_context,
                    is_first_slice=is_first_slice,
                    slices_since_last_pub=slices_since_last_pub,
                    total_written_ch=total_written_ch,
                    pending_parts=pending_parts,
                    new_rows_since_pub_by_part=new_rows_since_pub_by_part,
                    after_publish=_after_publish_update_state,
                )
                total_read += rows_read

            # Финальная публикация (и опциональный добор пропущенных партиций)
            if always_publish_at_end:
                _finalize_publication(
                    ch=ch,
                    cfg=cfg,
                    pg=pg,
                    process_name=process_name,
                    pending_parts=pending_parts,
                    since_dt=since_dt,
                    until_dt=until_dt,
                    backfill_missing_enabled=backfill_missing_enabled,
                    collect_missing_parts_between=lambda ch_, sdt, edt: _collect_raw_parts_between(ch_, cfg.CH_RAW_TABLE, sdt, edt),
                    new_rows_since_pub_by_part=new_rows_since_pub_by_part,
                )
    finally:
        # NB: Пустой finally необходим для корректной формы try-блока.
        # Все реальные ошибки обрабатываются внутри цикла слайсов (per-slice) и выше
        # по стеку (_run_etl_impl/main). Здесь мы не перехватываем исключения,
        # чтобы не скрывать причины сбоев; пустой finally не влияет на производительность.
        pass
    return total_read, total_written_ch


def _log_window_hints(used_watermark: bool, auto_until: bool, auto_window_hours: int) -> None:
    """
    Компактный помощник для печати подсказок по окну запуска.
    Выносит мелкие ветвления из _run_etl_impl, чтобы снизить когнитивную сложность
    без влияния на производительность.
    """
    try:
        if used_watermark:
            log.info("since не указан: использую watermark из журнала (inc_process_state).")
        if auto_until:
            log.info("until не указан: использую since + %d ч (RUN_WINDOW_HOURS).", int(auto_window_hours))
    except Exception:
        # Логирование — best effort
        pass


def _close_quietly(*resources: object) -> None:
    """
    Закрывает переданные объекты, если у них есть метод `close()`.
    Любые исключения при закрытии подавляются — это финальный этап, не критичный для результата.
    Вынесено из _run_etl_impl для снижения когнитивной сложности (меньше try-блоков).
    """
    for r in resources:
        if r is None:
            continue
        try:
            close = getattr(r, "close", None)
            if callable(close):
                close()
        except Exception:
            # Ничего: на этапе завершения мы не должны «ронять» процесс
            pass

#
# --- Мелкие хелперы для снижения когнитивной сложности _run_etl_impl ---

def _resolve_process_name(cfg: "Settings", manual: bool) -> str:
    """
    Возвращает имя процесса для журнала с учётом режима.
    Вынос в отдельный хелпер убирает условную логику из _run_etl_impl
    (минус к S3776), накладных расходов нет.
    """
    base = str(getattr(cfg, "PROCESS_NAME", ""))
    return ("manual_" + base) if manual else base


def _make_startup_fail_handler(
    journal: "ProcessJournal",
    since_dt: datetime,
    until_dt: datetime,
    cfg: "Settings",
):
    """
    Фабрика колбэка для единообразной обработки стартовых ошибок компонентов.
    Отдельная функция вместо вложенной `def` в _run_etl_impl снижает когнитивную
    сложность основной функции (Sonar S3776) и делает код читабельнее.
    Возвращает замыкание `on_fail(component, exc, extra)`.
    """
    def _on_fail(component: str, exc: Exception, extra: Optional[Dict[str, Any]] = None) -> None:
        _startup_fail_with_journal(journal, since_dt, until_dt, cfg, component, exc, extra)
    return _on_fail

def _bootstrap_pg_and_window(
    cfg: "Settings",
    args: argparse.Namespace,
    process_name: str,
) -> Tuple[
    "PGClient",
    "ProcessJournal",
    datetime,
    datetime,
    bool,
    bool,
    int,
    str,
    Callable[[str, Exception, Optional[Dict[str, Any]]], None],
]:
    """
    Компактный бутстрап: подключение к Postgres+журналу и сбор окна запуска.
    Вынос в отдельный хелпер уменьшает ветвистость `_run_etl_impl` (снижаем S3776)
    без влияния на производительность: здесь только создание объектов и простая логика.
    Возвращает кортеж: (pg, journal, since, until, used_watermark, auto_until,
    auto_window_hours, business_tz_name, on_startup_fail).
    """
    pg, journal = _connect_pg_and_journal(cfg, process_name)
    since_dt, until_dt, used_watermark, auto_until, auto_window_hours, business_tz_name, _ = _build_time_window(cfg, args, journal)
    on_startup_fail = _make_startup_fail_handler(journal, since_dt, until_dt, cfg)
    return (
        pg,
        journal,
        since_dt,
        until_dt,
        used_watermark,
        auto_until,
        auto_window_hours,
        business_tz_name,
        on_startup_fail,
    )


def _init_backends_and_check(
    cfg: "Settings",
    on_startup_fail: Callable[[str, Exception, Optional[Dict[str, Any]]], None],
) -> Tuple["PhoenixClient", "CHClient"]:
    """
    Инициализация Phoenix и ClickHouse + ранняя проверка таблиц.
    Вынос в отдельный хелпер даёт -2 к когнитивной сложности `_run_etl_impl`,
    при этом путь остаётся «горячим» и предельно прямолинейным.
    """
    phx = _connect_phx_client(cfg, on_startup_fail)
    ch = _connect_ch_client(cfg, on_startup_fail)
    required_tables = _required_ch_tables(cfg)
    _ensure_ch_tables_or_fail(ch, cfg, required_tables, on_startup_fail)
    return phx, ch

# --- Новый вынесенный heavy ETL-процесс ---
def _run_etl_impl(args: argparse.Namespace) -> None:
    """
    Реализация ETL-оркестрации вынесена из `_run_etl` для снижения когнитивной сложности (Sonar S3776).
    """
    setup_logging(args.log_level)
    _reduce_noise_for_info_mode()
    cfg = Settings()
    _install_signal_handlers()

    manual_mode = bool(args.manual_start)
    process_name = _resolve_process_name(cfg, manual_mode)
    # Бэкфилл пропущенных партиций включаем только в ручном режиме
    backfill_missing_enabled = manual_mode

    phx: Optional[PhoenixClient] = None
    ch: Optional[CHClient] = None
    pg: Optional[PGClient] = None
    try:
        # PG + журнал + окно запуска + единый обработчик стартовых ошибок
        (
            pg,
            journal,
            since_dt,
            until_dt,
            used_watermark,
            auto_until,
            auto_window_hours,
            business_tz_name,
            on_startup_fail,
        ) = _bootstrap_pg_and_window(cfg, args, process_name)
        _log_window_hints(used_watermark, auto_until, auto_window_hours)

        # Подключения к Phoenix/CH и ранняя проверка критичных таблиц
        phx, ch = _init_backends_and_check(cfg, on_startup_fail)

        # Политика публикации
        publish_every_slices, always_publish_at_end, publish_only_if_new, publish_min_new_rows = _prepare_publish_flags(cfg, manual_mode)

        # Параметры Phoenix-окна (захлёст только на первом слайсе)
        overlap_delta = _resolve_phx_overlap(cfg)

        host = socket.gethostname()
        pid = os.getpid()
        step_min = _resolve_step_min(cfg)

        # Основной сценарий под lock журнала
        params = ExecParams(
            journal=journal,
            cfg=cfg,
            ch=ch,
            phx=phx,
            pg=pg,
            process_name=process_name,
            host=host,
            pid=pid,
            since_dt=since_dt,
            until_dt=until_dt,
            step_min=step_min,
            business_tz_name=business_tz_name,
            overlap_delta=overlap_delta,
            publish_every_slices=publish_every_slices,
            publish_only_if_new=publish_only_if_new,
            publish_min_new_rows=publish_min_new_rows,
            always_publish_at_end=always_publish_at_end,
            backfill_missing_enabled=backfill_missing_enabled,
        )
        total_read, total_written_ch = _execute_with_lock(params)

        log.info("Готово. Прочитано: %d | в CH записано: %d", total_read, total_written_ch)

    finally:
        _close_quietly(phx, ch, pg)

def _run_etl(args: argparse.Namespace) -> None:
    """Тонкая оболочка: делегирует выполнение в `_run_etl_impl` и сразу выходит."""
    _run_etl_impl(args)

def main():
    """
    Тонкая оболочка: только парсинг аргументов и делегирование в `_run_etl`.
    Это снижает когнитивную сложность `main()` (S3776) без влияния на «горячие» участки.
    """
    parser = argparse.ArgumentParser(description="Codes History Incremental (Phoenix→ClickHouse)")
    # Флаги сведены к минимуму для простоты эксплуатации:
    #  - --since/--until — бизнес-окно загрузки;
    #  - --manual-start — пометить запуск как ручной (для журнала);
    #  - --log-level — уровень логирования (по умолчанию INFO).
    parser.add_argument("--since", required=False, help="ISO. Наивные значения трактуются в BUSINESS_TZ (если задана), иначе в UTC. Пр: 2025-08-08T00:00:00")
    parser.add_argument("--until", required=False, help="ISO. Наивные значения трактуются в BUSINESS_TZ (если задана), иначе в UTC. Если не указан — используется since + RUN_WINDOW_HOURS (по умолчанию 24 ч). Пр: 2025-08-09T00:00:00")
    parser.add_argument("--manual-start", action="store_true", help="Если указан, журнал ведётся под именем 'manual_<PROCESS_NAME>' (ручной запуск).")
    parser.add_argument("--log-level", "--log", dest="log_level", default="INFO", help="Уровень логирования (alias: --log)")

    args = parser.parse_args()
    _run_etl(args)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        _log = logging.getLogger("codes_history_increment")
        msg = "Остановлено пользователем (Ctrl+C)." if (_shutdown_reason in ("", "SIGINT")) else f"Остановлено сигналом {_shutdown_reason}."
        _log.warning("%s Выход с кодом 130.", msg)
        raise SystemExit(130)
    except Exception as unhandled:
        # На этом этапе cfg может быть ещё не создан — ориентируемся на ENV/уровень логгера
        want_trace = _want_trace(None)
        _log = logging.getLogger("codes_history_increment")
        _msg = f"Необработанная ошибка: {unhandled.__class__.__name__}: {unhandled}"
        if want_trace:
            _log.exception(_msg)
        else:
            _log.error(_msg + " (стек скрыт; установите ETL_TRACE_EXC=1 или запустите с --log-level=DEBUG, чтобы напечатать трейсбек)")
        raise SystemExit(1)