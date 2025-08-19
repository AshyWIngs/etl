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
from typing import Any, Dict, List, Tuple, Iterable, Optional, Set, cast
from zoneinfo import ZoneInfo

from .logging_setup import setup_logging
from .config import Settings
from .slicer import iter_slices
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient, PGConnectionError
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal
from time import perf_counter, sleep
from functools import lru_cache
from urllib.parse import urlparse
import random

log = logging.getLogger("codes_history_increment")

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

def _is_phx_overloaded(exc: BaseException) -> bool:
    """Возвращает True, если ошибка похожа на перегрузку Phoenix JobManager/очереди задач.
    Сигнатуры, на которые ориентируемся: RejectedExecutionException / JobManager ... rejected.
    """
    try:
        s = f"{exc}"
    except Exception:
        return False
    s_low = s.lower()
    return ("rejectedexecutionexception" in s_low) or ("jobmanager" in s and "rejected" in s_low)

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

def _to_dt64_obj(v: Any) -> Any:
    if v is None or v == "":
        return None
    dt: Optional[datetime] = None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, (int, float)):
        ts = float(v)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        dt = datetime.utcfromtimestamp(ts)
    else:
        s = str(v).strip()
        if not s:
            return None
        if s.isdigit():
            ts = int(s)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            dt = datetime.utcfromtimestamp(ts)
        else:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                return None
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)

def _as_int(v: Any) -> Any:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None

def _parse_ch_storage(v: Any) -> List[str]:
    """
    Быстрый парсер для поля-хранилища (массив строк в CH):
    • Горячий путь: уже list/tuple → лёгкая нормализация без JSON.
    • Пустые/«псевдопустые» значения → пустой список.
    • Строки вида '[...]' пробуем разобрать как JSON-список (узкий кейс).
    • Остальные строки разбиваем по простым разделителям без regex (быстрее).
    """
    # 1) Пустые/«псевдопустые» — сразу выход
    if v in (None, "", "{}", "[]"):
        return []

    # 2) Уже массив — самый дешёвый путь
    if isinstance(v, (list, tuple)):
        res: List[str] = []
        append = res.append
        for x in v:
            if x is None:
                continue
            xs = str(x).strip()
            if xs:
                append(xs)
        return res

    # 3) Приведём к строке один раз
    s = str(v).strip()
    if not s:
        return []

    # 4) Узкий быстрый путь: JSON-список вида "[...]" (без лишних попыток)
    if s[0:1] == "[" and s[-1:] == "]":
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                res: List[str] = []
                append = res.append
                for x in parsed:
                    if x is None:
                        continue
                    xs = str(x).strip()
                    if xs:
                        append(xs)
                return res
        except Exception:
            # Падаем в простой сплит ниже
            pass

    # 5) Универсальный быстрый сплит по распространённым разделителям (без regex)
    #    Нормализуем разделители к запятой, убираем крайние скобки.
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

def main():
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

    setup_logging(args.log_level)
    _reduce_noise_for_info_mode()
    cfg = Settings()
    _install_signal_handlers()

    process_name = ("manual_" + str(getattr(cfg, "PROCESS_NAME", ""))) if args.manual_start else cfg.PROCESS_NAME
    # Manual mode: отключаем гейтинг публикации и включаем бэкфилл «пропущенных» дней (за окно запуска)
    manual_mode = bool(args.manual_start)

    if manual_mode:
        # В ручном режиме включаем backfill пропущенных дней (только в финале запуска).
        # Гейтинг публикации отключается ниже через publish_only_if_new = ... and (not manual_mode).
        backfill_missing_enabled = True
    else:
        backfill_missing_enabled = False
    try:
        pg = PGClient(cfg.PG_DSN)
    except PGConnectionError as e:
        _log_maybe_trace(logging.CRITICAL, f"FATAL: postgres connect/init failed: {e}", exc=e, cfg=cfg)
        raise SystemExit(2)
    except Exception as e:
        _log_maybe_trace(logging.CRITICAL, f"FATAL: unexpected error during postgres init: {e.__class__.__name__}: {e}", exc=e, cfg=cfg)
        raise SystemExit(2)

    journal = ProcessJournal(cast(Any, pg), cfg.JOURNAL_TABLE, process_name)  # Приведение типов для Pylance: наш PGClient совместим по факту
    journal.ensure()

    # Безопасно создаём/обновляем строку состояния процесса (если поддерживается журналом).
    try:
        if hasattr(journal, "_state_upsert"):
            journal._state_upsert(status='idle', healthy=None, extra={})
    except Exception:
        # Не препятствуем запуску, если bootstrap состояния не удался
        pass

    # ---- БИЗНЕС-ИНТЕРВАЛ ЗАПУСКА ----
    # Упрощаем модель времени:
    #  • Если cfg.BUSINESS_TZ непустой → наивные CLI-датавремена трактуем в этом поясе ("business" режим).
    #  • Если cfg.BUSINESS_TZ пустая/отсутствует → считаем, что наивные значения заданы в UTC ("utc" режим).
    # Это убирает отдельную переменную INPUT_TZ, поведение становится однозначным.
    business_tz_raw = str(getattr(cfg, "BUSINESS_TZ", "") or "").strip()
    business_tz_name = business_tz_raw or "UTC"
    input_tz_mode = "business" if business_tz_raw else "utc"
    # since: если не задан, используем watermark (он уже в UTC)
    used_watermark = False
    if args.since:
        since_dt = _parse_cli_dt(args.since, input_tz_mode, business_tz_name)
    else:
        since_dt = getattr(journal, "get_watermark", lambda: None)() or None
        used_watermark = True
    if since_dt is None:
        raise SystemExit("Не задан --since и нет watermark в журнале")
    # until: если не указан — берём since + RUN_WINDOW_HOURS (из Settings/ENV), иначе разбираем из CLI
    auto_until = False
    _auto_window_hours = 0
    if args.until:
        until_dt = _parse_cli_dt(args.until, input_tz_mode, business_tz_name)
        if until_dt is None:
            raise SystemExit("Не удалось разобрать --until (проверьте формат даты/времени)")
    else:
        # Читаем окно по умолчанию; допускаем оба имени для совместимости
        try:
            win_h_raw = getattr(cfg, "RUN_WINDOW_HOURS", None)
            if win_h_raw is None:
                win_h_raw = getattr(cfg, "RUN_WINDOW_HOURS", None)
            window_hours = int(win_h_raw) if win_h_raw is not None else 24
        except Exception:
            window_hours = 24
        if window_hours <= 0:
            window_hours = 24
        until_dt = since_dt + timedelta(hours=window_hours)
        auto_until = True
        _auto_window_hours = window_hours

    # На этом этапе since_dt уже проверен выше (если None — мы бы завершились)
    if until_dt <= since_dt:
        raise SystemExit(f"Некорректное окно: since({since_dt.isoformat()}) >= until({until_dt.isoformat()})")
    # Узкие assert'ы для type checker (Pylance): ниже обе даты гарантированно не None
    assert isinstance(since_dt, datetime)
    assert isinstance(until_dt, datetime)

    step_min = int(getattr(cfg, "STEP_MIN", 60))
    # Отдельный сдвиг к Phoenix больше не используем: окно запроса = бизнес-окну

    if used_watermark:
        log.info("since не указан: использую watermark из журнала (inc_process_state).")
    if auto_until:
        log.info("until не указан: использую since + %d ч (RUN_WINDOW_HOURS).", _auto_window_hours)

    # Вспомогательная функция стартовой ошибки
    def _startup_fail(component: str, exc: Exception, extra: Optional[Dict[str, Any]] = None) -> None:
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
                    extra=extra or {}
                )
        except Exception:
            pass
        raise SystemExit(2)

    # ---- КОННЕКТЫ ----
    phx: Optional[PhoenixClient] = None
    ch: Optional[CHClient] = None
    try:
        phx = PhoenixClient(cfg.PQS_URL, fetchmany_size=cfg.PHX_FETCHMANY_SIZE, ts_units='timestamp')  # ts_units зафиксирован: 'timestamp'
    except Exception as e:
        _startup_fail("phoenix", e, extra={"url": cfg.PQS_URL})

    try:
        _u = urlparse(cfg.PQS_URL)
        _host = _u.hostname or cfg.PQS_URL
        _port = f":{_u.port}" if _u.port else ""
        log.info("Phoenix(PQS) подключён (%s%s)", _host, _port)
    except Exception:
        # На случай нестандартного URL хотя бы сообщим, что клиент готов
        log.info("Phoenix(PQS) клиент инициализирован")

    try:
        ch  = CHClient(
            hosts=cfg.ch_hosts_list(),
            port=cfg.CH_PORT,
            database=cfg.CH_DB,
            user=cfg.CH_USER,
            password=cfg.CH_PASSWORD,
            compression=True,
        )
    except Exception as e:
        _startup_fail("clickhouse", e, extra={"hosts": cfg.ch_hosts_list(), "db": cfg.CH_DB})
    try:
        log.info("ClickHouse: db=%s, cluster=%s, hosts=%s", cfg.CH_DB, getattr(cfg, "CH_CLUSTER", None), ",".join(cfg.ch_hosts_list()))
    except Exception:
        pass

    # Static type-narrowing for editors/type-checkers: к этому моменту коннекты гарантированно установлены
    assert phx is not None, "phoenix client not initialized"
    assert ch is not None, "clickhouse client not initialized"

    host = socket.gethostname()
    pid = os.getpid()

    # Счётчики/аккумуляторы текущего запуска
    total_read = 0
    total_written_ch = 0
    pending_parts: Set[int] = set()
    slices_since_last_pub = 0
    _new_rows_since_pub_by_part: Dict[int, int] = defaultdict(int)

    ch_table_raw_all   = cfg.CH_RAW_TABLE
    ch_table_clean     = cfg.CH_CLEAN_TABLE
    ch_table_clean_all = cfg.CH_CLEAN_ALL_TABLE
    ch_table_buf       = cfg.CH_DEDUP_BUF_TABLE
    ch_batch = int(cfg.CH_INSERT_BATCH)

    required_tables = [ch_table_raw_all, ch_table_clean, ch_table_clean_all, ch_table_buf]
    try:
        ch.ensure_tables(
            required_tables,
            db=cfg.CH_DB,
            cluster=getattr(cfg, "CH_CLUSTER", None),
            try_switch_host=True
        )
    except Exception as ex:
        _startup_fail(
            "clickhouse",
            ex,
            extra={
                "stage": "ensure_tables",
                "db": cfg.CH_DB,
                "hosts": cfg.ch_hosts_list(),
                "required": required_tables,
            },
        )

    phx_overlap_min = int(getattr(cfg, "PHX_QUERY_OVERLAP_MINUTES", 0) or 0)
    overlap_only_first_slice = True  # фиксированная политика: захлёст только на первом слайсе
    overlap_delta = timedelta(minutes=phx_overlap_min) if phx_overlap_min > 0 else timedelta(0)

    publish_every_slices = int(getattr(cfg, "PUBLISH_EVERY_SLICES", 0) or 0)
    # Всегда делаем финальную публикацию — это «точка согласования».
    # Конфиг ALWAYS_PUBLISH_AT_END больше не читаем: поведение зафиксировано для предсказуемости.
    always_publish_at_end = True

    # Управление публикацией:
    # В обычном режиме допускаем гейтинг по «новизне»; в manual-режиме он отключён.
    publish_only_if_new = bool(int(getattr(cfg, "PUBLISH_ONLY_IF_NEW", 1))) and (not manual_mode)
    publish_min_new_rows = int(getattr(cfg, "PUBLISH_MIN_NEW_ROWS", 1))

    def _collect_missing_parts_between(ch: CHClient, start_dt: datetime, end_dt: datetime) -> Set[int]:
        """
        Лёгкий стартовый backfill только для manual-режима:
        возвращает набор партиций (UTC YYYYMMDD) из RAW, у которых в окне [start_dt; end_dt) есть строки.
        Выполняем один агрегирующий запрос по whole-window, чтобы не делать N маленьких запросов.
        Если что-то пойдёт не так — возвращаем пустое множество (не фейлим весь запуск).
        """
        try:
            s_str = start_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            e_str = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            sql = (
                f"SELECT toYYYYMMDD(opd) AS p, count() AS cnt "
                f"FROM {ch_table_raw_all} "
                f"WHERE opd >= toDateTime('{s_str}') AND opd < toDateTime('{e_str}') "
                f"GROUP BY p"
            )
            rows = ch.execute(sql) or []
            return {int(p) for (p, cnt) in rows if int(cnt) > 0}
        except Exception as ex:
            log.warning("Backfill: не удалось собрать партиции за окно: %s", ex)
            return set()

    def _publish_parts(ch: CHClient, parts: Set[int], *, force: bool = False) -> bool:
        """
        Дедуп/публикация набора партиций.
        Схема: DROP BUF → INSERT BUF (GROUP BY c,t,opd с argMax(..., ingested_at)) → REPLACE PARTITION → DROP BUF.
        Если `force=False`, предполагается, что гейтинг по «новизне» уже отработал на уровне вызывающего кода.
        Возвращает True, если была выполнена хотя бы одна REPLACE.
        """
        if not parts:
            return False

        on_cluster = ""
        try:
            cluster_name = getattr(cfg, "CH_CLUSTER", None)
            if cluster_name:
                on_cluster = f" ON CLUSTER {cluster_name}"
        except Exception:
            pass

        published_any = False
        parts_sorted = sorted(set(parts))

        # Отдельная запись в журнале для этапа дедупа/публикации
        try:
            dedup_journal = ProcessJournal(cast(Any, pg), cfg.JOURNAL_TABLE, f"{process_name}:dedup")  # Приведение типов для Pylance: PGClient реализует нужные методы на практике
            dedup_journal.ensure()
        except Exception:
            dedup_journal = None

        pub_start, pub_end = _parts_to_interval(parts_sorted)
        run_id = None
        try:
            if dedup_journal:
                run_id = dedup_journal.mark_running(pub_start, pub_end, host=host, pid=pid)
        except Exception:
            run_id = None

        for p in parts_sorted:
            try:
                # 1) Чистим буферную партицию
                try:
                    ch.execute(f"ALTER TABLE {ch_table_buf}{on_cluster} DROP PARTITION {p}")
                except Exception:
                    # если нечего дропать — идём дальше
                    pass

                # 2) Дедупим в буфер
                #    ВАЖНО: перечисляем только «пользовательские» колонки (без ingested_at) —
                #    для него сработает DEFAULT now('UTC'). Алиасы/материализованные колонки
                #    (opd_local*, *_local и т.п.) ClickHouse заполнит автоматически.
                insert_sql = (
                    f"INSERT INTO {ch_table_buf} ({CH_COLUMNS_STR}) "
                    f"SELECT {DEDUP_SELECT_COLS} "
                    f"FROM {ch_table_raw_all} "
                    f"WHERE toYYYYMMDD(opd) = {p} "
                    f"GROUP BY c, t, opd"
                )
                ch.execute(insert_sql)

                # 3) Публикация: REPLACE PARTITION из буфера в CLEAN
                replace_sql = f"ALTER TABLE {ch_table_clean}{on_cluster} REPLACE PARTITION {p} FROM {ch_table_buf}"
                ch.execute(replace_sql)
                published_any = True

            except Exception as ex:
                # Частый кейс: в буфере ничего не вставилось (0 строк) — REPLACE даст ошибку «нет партиции».
                # Это нормально для «пустых» дней. Не считаем это фаталом — просто логируем.
                _log_maybe_trace(logging.WARNING, f"Публикация партиции {p} пропущена: {ex}", exc=ex, cfg=cfg)
            finally:
                # 4) Убираем временную партицию в буфере независимо от исхода REPLACE
                try:
                    ch.execute(f"ALTER TABLE {ch_table_buf}{on_cluster} DROP PARTITION {p}")
                except Exception:
                    pass

        # Финализируем этап в журнале
        try:
            if dedup_journal:
                if published_any:
                    dedup_journal.mark_done(pub_start, pub_end, rows_read=0, rows_written=0)
                else:
                    # Нет ни одной успешной публикации — считаем запуск «ОК без изменений»
                    dedup_journal.mark_done(pub_start, pub_end, rows_read=0, rows_written=0)
        except Exception:
            pass

        return published_any

    def _maybe_publish_after_slice(ch: CHClient) -> None:
        nonlocal slices_since_last_pub
        # Публикуем строго по числу успешных слайсов.
        if publish_every_slices > 0 and slices_since_last_pub >= publish_every_slices:
            # Диагностика гейтинга (печатаем только когда реально включён DEBUG)
            if log.isEnabledFor(logging.DEBUG):
                try:
                    _dbg_map = {str(p): int(_new_rows_since_pub_by_part.get(p, 0)) for p in sorted(pending_parts)}
                    log.debug("Гейтинг: pending_parts=%s; new_rows_by_part=%s; threshold=%d; slices=%d/%d",
                            ",".join(str(p) for p in sorted(pending_parts)) or "-",
                            json.dumps(_dbg_map, ensure_ascii=False),
                            publish_min_new_rows, slices_since_last_pub, publish_every_slices)
                except Exception:
                    pass
            # Если требуется «новизна», публикуем только те партиции,
            # по которым в рамках ТЕКУЩЕГО запуска реально есть новые строки.
            parts_to_publish = set(pending_parts)
            if publish_only_if_new:
                parts_to_publish = {p for p in parts_to_publish if _new_rows_since_pub_by_part.get(p, 0) >= publish_min_new_rows}

            if parts_to_publish:
                published = _publish_parts(ch, parts_to_publish, force=not publish_only_if_new)
                if published:
                    log.info("Промежуточная публикация: %d партиций.", len(parts_to_publish))
                    # Сбрасываем счётчики «новизны» и удаляем опубликованные партиции из pending.
                    for p in parts_to_publish:
                        _new_rows_since_pub_by_part[p] = 0
                        pending_parts.discard(p)
                    slices_since_last_pub = 0
                else:
                    log.debug(
                        "Промежуточная публикация пропущена: parts_to_publish=%s не прошли гейтинг.",
                        ",".join(str(p) for p in sorted(parts_to_publish)),
                    )
            else:
                # Порог новых строк не достигнут — фиксируем это на уровне INFO
                log.info(
                    "Промежуточная публикация отложена гейтингом (порог=%d, слайсы=%d/%d).",
                    publish_min_new_rows, slices_since_last_pub, publish_every_slices,
                )

    try:
        with journal.exclusive_lock() as got:
            if not got:
                log.warning("Другой инстанс '%s' уже выполняется — выходим.", process_name)
                return

            # === Санация "зависших" запусков ===========================================
            # ВАЖНО: это НЕ ретенция данных (её делает journal партициями), а логическая
            # санация актуального состояния: переводим "planned" старше N минут в "skipped",
            # "running" без heartbeat старше M минут — в "error". Это позволяет новому
            # запуску безболезненно продолжить работу, даже если предыдущий умер.
            # TTL здесь лишь для статусов, а не для физической очистки строк.
            journal.sanitize_stale(
                planned_ttl_minutes=60,
                running_heartbeat_timeout_minutes=45,
                running_hard_ttl_hours=12,
            )

            # --- Стартовый бэкфилл последних N дней (защита от падений между запуском и публикацией) ---
            # Если предыдущий запуск упал после вставки в RAW и до публикации в CLEAN,
            # при старте допубликуем недостающие партиции за недавний период.
            # Лёгкая и безопасная операция: REPLACE PARTITION идемпотентен.
            try:
                backfill_days = int(getattr(cfg, "STARTUP_BACKFILL_DAYS", 0) or 0)
            except Exception:
                backfill_days = 0
            if backfill_days > 0:
                # Опорной точкой берём правую границу текущего окна запуска (until_dt).
                bf_end = until_dt
                bf_start = bf_end - timedelta(days=backfill_days)
                try:
                    parts_recent = _collect_missing_parts_between(ch, bf_start, bf_end)
                except Exception as ex:
                    parts_recent = set()
                    _log_maybe_trace(logging.WARNING, f"Стартовый бэкфилл: не удалось собрать партиции за {backfill_days} дн.: {ex}", exc=ex, cfg=cfg)
                if parts_recent:
                    log.info("Стартовый бэкфилл: допубликую %d партиций за последние %d дней.", len(parts_recent), backfill_days)
                    # Публикуем без гейтинга «новизны» — спасение после возможного падения.
                    _publish_parts(ch, parts_recent, force=True)
                else:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug("Стартовый бэкфилл: в RAW нет данных за последние %d дней — пропускаю.", backfill_days)

            logged_tz_context = False
            is_first_slice = True
            # === Чтение из Phoenix и запись в RAW ======================================
            # Каждый слайс: planned → running → чтение Phoenix батчами → INSERT VALUES в RAW
            # → heartbeat прогресса → накопление pending_parts (UTC-дни) → mark_done →
            # условная промежуточная публикация по порогу слайсов.
            for s, e in iter_slices(since_dt, until_dt, step_min):
                _check_stop()
                s_q_base = s
                e_q_base = e

                use_overlap = overlap_delta if (is_first_slice or not overlap_only_first_slice) else timedelta(0)
                s_q = s_q_base - use_overlap if use_overlap > timedelta(0) else s_q_base
                e_q = e_q_base  # без правого лага
                # ВАЖНО: s_q/e_q — это именно UTC-aware времена бизнес-окна;
                # Phoenix ниже получает naive UTC (tzinfo=None) для параметров запроса.

                # небольшой контекст TZ для логов (информативно; партиции — всегда UTC)
                try:
                    _s_aware = s if s.tzinfo else s.replace(tzinfo=timezone.utc)
                    _biz_offset_min = int(((_s_aware.astimezone(_get_tz(business_tz_name))).utcoffset() or timedelta(0)).total_seconds() // 60)
                except Exception:
                    _biz_offset_min = 0

                # Однократный лог TZ context
                if not logged_tz_context:
                    log.info("TZ context: business_tz=%s, business_offset=%+d мин; partitions=UTC",
                             business_tz_name, _biz_offset_min)
                    logged_tz_context = True

                # лог окна (в INFO не шумим — подробности только в DEBUG)
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
                # --- Heartbeat троттлинг для надёжности долгих слайсов ---
                # Обновляем состояние процесса не чаще, чем раз в JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC,
                # чтобы не «топить» Postgres и при этом показывать живой прогресс.
                hb_interval_sec = int(getattr(cfg, "JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", 300) or 300)
                if hb_interval_sec < 5:  # хард-сейфти на случай слишком малого значения
                    hb_interval_sec = 5
                _next_hb_deadline = perf_counter() + hb_interval_sec
                # Стартовый heartbeat (необязательно, но полезно для наблюдения)
                try:
                    journal.heartbeat(run_id, progress={"rows_read": 0, "rows_written": total_written_ch})
                except Exception:
                    # heartbeat — best effort, падать из‑за него не нужно
                    pass

                rows_read = 0
                ch_rows: List[tuple] = []

                try:
                    s_q_phx = s_q.replace(tzinfo=None) if s_q.tzinfo else s_q
                    e_q_phx = e_q.replace(tzinfo=None) if e_q.tzinfo else e_q

                    # Чтение Phoenix с простым экспоненциальным ретраем на перегрузке JobManager
                    max_attempts = 4  # фиксированно, без конфигурации: держим код простым
                    attempt = 1
                    while True:
                        try:
                            # Сбрасываем локальные счётчики для попытки
                            rows_read = 0
                            ch_rows = []

                            # Phoenix fetch loop
                            for batch in _iter_phx_batches(phx, cfg, s_q_phx, e_q_phx):
                                _check_stop()
                                # Пустой батч возможен при редких глитчах на стороне PQS — не считаем его ошибкой.
                                if not batch:
                                    nowp = perf_counter()
                                    if nowp >= _next_hb_deadline:
                                        try:
                                            journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})
                                        except Exception:
                                            pass
                                        _next_hb_deadline = nowp + hb_interval_sec
                                    continue

                                rows_read += len(batch)
                                # Одним проходом: считаем «новую» дельту по партициям и формируем кортежи для INSERT
                                for r in batch:
                                    p = _opd_to_part_utc(r.get("opd"))
                                    if p is not None:
                                        _new_rows_since_pub_by_part[p] += 1
                                    ch_rows.append(_row_to_ch_tuple(r))

                                # Чанковая вставка в CH по порогу, чтобы не раздувать память на больших окнах
                                if ch_batch > 0 and len(ch_rows) >= ch_batch:
                                    total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                                    ch_rows.clear()
                                    # heartbeat по факту записи — фиксируем прогресс
                                    nowp = perf_counter()
                                    if nowp >= _next_hb_deadline:
                                        try:
                                            journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})
                                        except Exception:
                                            pass
                                        _next_hb_deadline = nowp + hb_interval_sec

                            # Финальный флаш оставшегося буфера + heartbeat
                            if ch_rows:
                                total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                                ch_rows.clear()
                                nowp = perf_counter()
                                if nowp >= _next_hb_deadline:
                                    try:
                                        journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})
                                    except Exception:
                                        pass
                                    _next_hb_deadline = nowp + hb_interval_sec

                            # Успех — выходим из retry-цикла
                            break

                        except Exception as ex:
                            # Если это перегрузка PQS/JobManager — делаем мягкий ретрай с экспоненциальной паузой
                            if _is_phx_overloaded(ex) and attempt < max_attempts:
                                # NB: если часть батчей уже была вставлена в RAW до ошибки, возможны дубли при повторе слайса.
                                # Это допустимо: публикация (REPLACE PARTITION) в CLEAN идемпотентна и устранит дубли по argMax.
                                backoff = (2.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)
                                log.warning(
                                    "Phoenix перегружен (JobManager queue). Повторю попытку %d/%d через %.1f с...",
                                    attempt + 1, max_attempts, backoff,
                                )
                                try:
                                    journal.heartbeat(run_id, progress={"retry": attempt, "last_error": str(ex)[:300]})
                                except Exception:
                                    pass
                                sleep(backoff)
                                attempt += 1
                                continue
                            # Иначе — отдаём ошибку наверх (будет mark_error на уровне обработчика слайса)
                            raise

                    # накопим партиции текущего слайда — СТРОГО ПО UTC
                    for p in _iter_partitions_by_day_tz(s_q, e_q, "UTC"):
                        pending_parts.add(p)
                    slices_since_last_pub += 1
                    is_first_slice = False

                    # ВАЖНО: выполняем гейтинг/промежуточную публикацию ДО mark_done(),
                    # чтобы возможные ошибки корректно зафиксировались как error по текущему слайсу,
                    # а не «после завершения».
                    _maybe_publish_after_slice(ch)

                    journal.mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
                    total_read += rows_read

                    # Строковое представление набора партиций для лаконичного лога
                    _pp = ",".join(str(p) for p in sorted(pending_parts)) or "-"
                    log.info(
                        "Слайс завершён: %s → %s (PHX: %s → %s); rows_read=%d, rows_written_raw_total=%d, pending_parts=%s",
                        s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
                        rows_read, total_written_ch, _pp,
                    )

                except KeyboardInterrupt:
                    try:
                        _mark_cancelled = getattr(journal, "mark_cancelled", None)
                        if callable(_mark_cancelled):
                            _mark_cancelled(s, e, message=_interrupt_message())
                        else:
                            journal.mark_error(s, e, message=_interrupt_message())
                    except Exception:
                        pass
                    raise
                except Exception as ex:
                    journal.heartbeat(run_id, progress={"error": str(ex)})
                    journal.mark_error(s, e, message=str(ex))
                    raise

            # === Финальная публикация и опциональный бэкфилл ============================
            # В финале всегда публикуем pending_parts (это "точка согласования").
            # В manual-режиме дополнительно добираем пропущенные дни за окно запуска.
            if always_publish_at_end:
                # Дополнительно захватим «пропущенные» дни за окно запуска, если это включено.
                if backfill_missing_enabled:
                    try:
                        missing_parts = _collect_missing_parts_between(ch, since_dt, until_dt)
                        for mp in (missing_parts or []):
                            pending_parts.add(mp)
                    except Exception as e:
                        log.warning("Backfill: сбор пропущенных партиций завершился с ошибкой: %s", e)

                if pending_parts:
                    log.info("Финальная публикация: %d партиций.", len(pending_parts))
                    # В финале публикуем без гейтинга — это «точка согласования».
                    _published_now = set(pending_parts)
                    _publish_parts(ch, _published_now, force=True)
                    # Сбрасываем счётчик «новизны» по опубликованным партициям и чистим pending
                    for _p in _published_now:
                        _new_rows_since_pub_by_part[_p] = 0
                        pending_parts.discard(_p)
                else:
                    log.info("Финальная публикация: нечего публиковать.")


        log.info("Готово. Прочитано: %d | в CH записано: %d", total_read, total_written_ch)

    finally:
        try:
            if phx is not None:
                phx.close()
        except Exception:
            pass
        try:
            if ch is not None:
                ch.close()
        except Exception:
            pass
        try:
            pg.close()
        except Exception:
            pass

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
    
