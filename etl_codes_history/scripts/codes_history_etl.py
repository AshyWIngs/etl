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
  только «захлёст» назад PHX_QUERY_OVERLAP_MINUTES на первом (или каждом) слайсе.
  Правую границу не «замораживаем».
• DateTime64(3) передаём как python datetime (naive), микросекунды обрезаем до миллисекунд.
• Никаких CSV — сразу INSERT в ClickHouse (native, compression).

Каденс публикаций
-----------------
• Публикация управляется ТОЛЬКО числом успешных слайсов: PUBLISH_EVERY_SLICES.
• В конце запуска выполняется финальная публикация, если ALWAYS_PUBLISH_AT_END=1.
• Для защиты от «пустых» публикаций действует лёгкий гейтинг:
  PUBLISH_ONLY_IF_NEW=1 и порог PUBLISH_MIN_NEW_ROWS (по count() в RAW на партицию).

Как это работает
----------------
1) Разбиваем окно [--since, --until) на слайды по STEP_MIN минут (UTC-времена формируются из CLI согласно INPUT_TZ и BUSINESS_TZ из конфига).
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
-----------------
--since / --until  — окно обработки.
--manual-start     — ручной запуск (журнал: manual_<PROCESS_NAME>).
--log-level        — уровень логирования (по умолчанию INFO).
"""
from __future__ import annotations

import argparse
import logging
import os
import json
import socket
import signal
from threading import Event
from psycopg.errors import UniqueViolation
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Iterable, Optional, Set
from zoneinfo import ZoneInfo

from .logging_setup import setup_logging
from .config import Settings
from .slicer import iter_slices
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient, PGConnectionError
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal
from clickhouse_driver import errors as ch_errors
from time import perf_counter
from functools import lru_cache

log = logging.getLogger("codes_history_increment")

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
            msg = msg + " (stack suppressed; set ETL_TRACE_EXC=1 или --log-level=DEBUG для traceback)"
        log.log(level, msg)

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
        return f"Interrupted by signal ({_shutdown_reason})"
    return "Interrupted by user (SIGINT)"

def _check_stop() -> None:
    if shutdown_event.is_set():
        raise KeyboardInterrupt()

# Порядок колонок должен 1-в-1 совпадать с DDL stg.daily_codes_history
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
# Предвычисления для дедупа (ускоряет и упрощает код публикации)
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
        log.warning("Unknown TZ='%s', falling back to UTC", tz_name)
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
    - mode='utc'      — наивные строки трактуем как UTC (как раньше), aware → приводим к UTC.
    - mode='business' — наивные строки трактуем в BUSINESS_TZ, затем конвертируем в UTC.
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

# ------------------------ ОСНОВНОЙ СЦЕНАРИЙ ------------------------

def main():
    parser = argparse.ArgumentParser(description="Codes History Incremental (Phoenix→ClickHouse)")
    # Флаги сведены к минимуму для простоты эксплуатации:
    #  - --since/--until — бизнес-окно загрузки;
    #  - --manual-start — пометить запуск как ручной (для журнала);
    #  - --log-level — уровень логирования (по умолчанию INFO).
    parser.add_argument("--since", required=False, help="ISO. Наивные значения трактуются согласно INPUT_TZ (из конфига). Пр: 2025-08-08T00:00:00")
    parser.add_argument("--until", required=True,  help="ISO. Наивные значения трактуются согласно INPUT_TZ (из конфига). Пр: 2025-08-09T00:00:00")
    parser.add_argument("--manual-start", action="store_true", help="Если указан, журнал ведётся под именем 'manual_<PROCESS_NAME>' (ручной запуск).")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = Settings()
    _install_signal_handlers()

    process_name = ("manual_" + str(getattr(cfg, "PROCESS_NAME", ""))) if args.manual_start else cfg.PROCESS_NAME

    # --- Инициализация Postgres (журнал) с понятным сообщением об ошибке ---
    try:
        pg = PGClient(cfg.PG_DSN)
    except PGConnectionError as e:
        _log_maybe_trace(logging.CRITICAL, f"FATAL: postgres connect/init failed: {e}", exc=e, cfg=cfg)
        raise SystemExit(2)
    except Exception as e:
        _log_maybe_trace(logging.CRITICAL, f"FATAL: unexpected error during postgres init: {e.__class__.__name__}: {e}", exc=e, cfg=cfg)
        raise SystemExit(2)

    journal = ProcessJournal(pg, cfg.JOURNAL_TABLE, process_name)
    journal.ensure()

    # ---- БИЗНЕС-ИНТЕРВАЛ ЗАПУСКА ----
    # Режим трактовки наивных дат берём строго из конфигурации (единый источник правды)
    input_tz_mode = getattr(cfg, "INPUT_TZ", "business")
    # Часовой пояс бизнеса — строго из конфигурации (без прямого чтения ENV)
    business_tz_name = getattr(cfg, "BUSINESS_TZ", "UTC")
    # since: если не задан, используем watermark (он уже в UTC)
    since_dt = _parse_cli_dt(args.since, input_tz_mode, business_tz_name) if args.since else journal.get_watermark() or None
    if since_dt is None:
        raise SystemExit("Не задан --since и нет watermark в журнале")
    # until всегда обязателен, разбираем согласно режиму
    until_dt = _parse_cli_dt(args.until, input_tz_mode, business_tz_name)
    if until_dt <= since_dt:
        raise SystemExit(f"Некорректное окно: since({since_dt.isoformat()}) >= until({until_dt.isoformat()})")

    step_min = int(getattr(cfg, "STEP_MIN", 60))
    # Отдельный сдвиг к Phoenix больше не используем: окно запроса = бизнес-окну

    log.info("Запуск: input_tz=%s, business_tz=%s, step_min=%d; since(UTC)=%s, until(UTC)=%s",
            input_tz_mode, business_tz_name, step_min, since_dt.isoformat(), until_dt.isoformat())

    # Вспомогательная функция стартовой ошибки
    def _startup_fail(component: str, exc: Exception, extra: Optional[Dict[str, Any]] = None) -> None:
        msg = f"{component} connect/init failed: {exc}"
        _log_maybe_trace(logging.ERROR, f"Стартовая ошибка компонента {component}: {msg}", exc=exc, cfg=cfg)
        try:
            journal.mark_startup_error(
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
    try:
        phx = PhoenixClient(cfg.PQS_URL, fetchmany_size=cfg.PHX_FETCHMANY_SIZE, ts_units=cfg.PHX_TS_UNITS)
    except Exception as e:
        _startup_fail("phoenix", e, extra={"url": cfg.PQS_URL})

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
    overlap_only_first_slice = bool(int(getattr(cfg, "PHX_OVERLAP_ONLY_FIRST_SLICE", 1)))
    overlap_delta_c = timedelta(minutes=phx_overlap_min) if phx_overlap_min > 0 else timedelta(0)

    publish_every_slices = int(getattr(cfg, "PUBLISH_EVERY_SLICES", 0) or 0)
    always_publish_at_end = bool(int(getattr(cfg, "ALWAYS_PUBLISH_AT_END", 1)))

    # Управление «молчаливой» публикацией берём из Settings(); дефолты держим здесь на случай отсутствия ключей в конфиге:
    publish_only_if_new = bool(int(getattr(cfg, "PUBLISH_ONLY_IF_NEW", 1)))
    publish_min_new_rows = int(getattr(cfg, "PUBLISH_MIN_NEW_ROWS", 1))

    _last_published_raw_count: Dict[int, int] = {}

    # Схема Phoenix
    try:
        available_columns = phx.discover_columns(cfg.HBASE_MAIN_TABLE)
    except Exception as e:
        log.exception("Не удалось прочитать схему Phoenix: %s", e)
        raise SystemExit(2)
    req_cols = cfg.main_columns_list()
    avail_map = {a.lower(): a for a in available_columns}
    effective: List[str] = []
    missing: List[str] = []
    for c in req_cols:
        k = c.strip().lower()
        if k in avail_map:
            effective.append(avail_map[k])
        else:
            missing.append(c)
    if missing:
        log.warning("В источнике отсутствуют колонки (будут пропущены): %s", ", ".join(missing))
    if not effective:
        raise SystemExit("После согласования колонок не осталось ни одной для SELECT — выход.")
    log.info("Итоговые колонки для SELECT (%d): %s", len(effective), ", ".join(effective))

    total_read = 0
    total_written_ch = 0

    host = socket.gethostname()
    pid = os.getpid()

    pending_parts: Set[int] = set()
    slices_since_last_pub = 0

    ch_dedup_settings = {
        "max_threads": 0,
        "max_memory_usage": 8 * 1024 * 1024 * 1024,                   # 8G
        "max_bytes_before_external_group_by": 2 * 1024 * 1024 * 1024,  # 2G
        "distributed_aggregation_memory_efficient": 1,
        # НИЖЕ — критично для вашего шардинга по 'c' и GROUP BY (c,t,opd):
        "optimize_distributed_group_by_sharding_key": 1,
        "prefer_localhost_replica": 1,
    }

    def _ch_exec(sql: str, *, settings: dict | None = None, label: str = "",
                 stage: str | None = None, part: int | None = None,
                 journal_pub: Optional[ProcessJournal] = None) -> int:
        t0 = perf_counter()
        try:
            ch.execute(sql, settings=settings)
        except ch_errors.UnexpectedPacketFromServerError as ex:
            log.warning("CH '%s' упал с UnexpectedPacketFromServerError: %s — reconnect() и повтор", label or "exec", ex)
            try:
                ch.reconnect()
            except Exception as re:
                log.warning("reconnect() перед повтором не удался: %s", re)
            ch.execute(sql, settings=settings)
        dur_ms = int((perf_counter() - t0) * 1000)
        ev = {"stage": stage or (label or "exec"), "part": part, "ms": dur_ms}
        log.info("Дедуп: %s (part=%s) занял %d мс", ev["stage"], str(ev["part"]), dur_ms)
        if journal_pub is not None:
            try:
                journal_pub.heartbeat(progress={"dedup_event": ev})
            except Exception as he:
                log.debug("journal heartbeat (dedup_event) не записан: %s", he)
        return dur_ms

    def _raw_count_for_part(part: int) -> Optional[int]:
        """
        Быстрый подсчёт строк в RAW для указанной партиции.
        Сначала через system.parts (сумма rows) по ЛОКАЛЬНОЙ таблице, на всём кластере;
        при ошибке — безопасный фоллбэк на SELECT count() по Distributed.
        """
        # Определим локальную таблицу из Distributed: stg.daily_codes_history_raw_all -> stg.daily_codes_history_raw
        local_table = ch_table_raw_all[:-4] if ch_table_raw_all.endswith("_all") else ch_table_raw_all
        if "." in local_table:
            db_name, tbl_name = local_table.split(".", 1)
        else:
            db_name, tbl_name = cfg.CH_DB, local_table

        part_str = str(part)
        cluster = (getattr(cfg, "CH_CLUSTER", "") or "").strip()

        try:
            if cluster:
                sql = (
                    f"SELECT sum(rows) "
                    f"FROM clusterAllReplicas('{cluster}', system.parts) "
                    f"WHERE database = '{db_name}' AND table = '{tbl_name}' "
                    f"  AND active AND partition = '{part_str}'"
                )
            else:
                sql = (
                    f"SELECT sum(rows) FROM system.parts "
                    f"WHERE database = '{db_name}' AND table = '{tbl_name}' "
                    f"  AND active AND partition = '{part_str}'"
                )
            res = ch.execute(sql)
            total = int(res[0][0]) if res and res[0][0] is not None else 0
            log.debug("raw_count(part=%s) via system.parts -> %d", part, total)
            return total
        except Exception as e:
            log.debug("system.parts count failed (%s), fallback to SELECT count() on Distributed.", e)
            try:
                sql_fb = f"SELECT count() FROM {ch_table_raw_all} WHERE toYYYYMMDD(opd) = {part}"
                res = ch.execute(sql_fb, settings={"prefer_localhost_replica": 1})
                total = int(res[0][0]) if res else 0
                log.debug("raw_count(part=%s) fallback Distributed -> %d", part, total)
                return total
            except Exception as e2:
                log.warning("count() RAW (fallback) для part=%s не получен: %s", part, e2)
                return None

    def _publish_parts(parts: Set[int], *, force: bool = False) -> None:
        nonlocal slices_since_last_pub
        _check_stop()
        if not parts:
            return

        if publish_only_if_new and not force:
            parts_raw_count: Dict[int, Optional[int]] = {}
            eligible: List[int] = []
            for p in sorted(parts):
                total = _raw_count_for_part(p)
                parts_raw_count[p] = total
                if total is None:
                    log.info("GATING part=%s: raw_count=NULL (error) -> publish for safety.", p)
                    eligible.append(p)
                    continue
                prev = _last_published_raw_count.get(p)
                if prev is None:
                    if total == 0:
                        log.info("GATING part=%s: raw_count=0, prev=∅ -> skip.", p)
                        continue
                    log.info("GATING part=%s: raw_count=%d, prev=∅ -> publish.", p, total)
                    eligible.append(p)
                    continue
                delta = total - prev
                if delta >= publish_min_new_rows:
                    log.info("GATING part=%s: raw=%d, prev=%d, delta=%d ≥ threshold(%d) -> publish.",
                             p, total, prev, delta, publish_min_new_rows)
                    eligible.append(p)
                else:
                    log.info("GATING part=%s: raw=%d, prev=%d, delta=%d < threshold(%d) -> skip.",
                             p, total, prev, delta, publish_min_new_rows)
            parts = set(eligible)
            if not parts:
                log.info("Дедуп/публикация: пропуск — по всем партициям нет новых строк относительно watermark.")
                slices_since_last_pub = 0
                return

        try:
            ch.reconnect()
            log.debug("CH reconnect before publish: OK")
        except Exception as e:
            log.warning("Не удалось переподключиться к CH перед публикацией: %s", e)

        pub_since, pub_until = _parts_to_interval(parts)
        journal_pub = ProcessJournal(pg, cfg.JOURNAL_TABLE, process_name + ":dedup")
        try:
            journal_pub.mark_planned(pub_since, pub_until)
            journal_pub.mark_running(pub_since, pub_until, host=host, pid=pid)
        except Exception as e:
            log.warning("Не удалось создать запись под-процесса для публикации: %s", e)
            journal_pub = None

        try:
            for part in sorted(parts):
                _check_stop()
                lock_key = f"codes_history:dedup:{part}"
                pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key,))
                got = bool(pg.fetchone()[0])
                if not got:
                    log.warning("Партиция %s: лок занят другим инстансом — пропускаю.", part)
                    continue
                try:
                    log.info("Партиция %s: начинаю дедуп/публикацию…", part)
                    part_t0 = perf_counter()

                    _ch_exec(f"ALTER TABLE {ch_table_buf} DROP PARTITION {part}",
                             label=f"drop buf partition {part}", stage="drop_buf", part=part, journal_pub=journal_pub)

                    insert_sql = f"""
                        INSERT INTO {ch_table_buf} ({CH_COLUMNS_STR})
                        SELECT
                        {DEDUP_SELECT_COLS}
                        FROM {ch_table_raw_all}
                        WHERE toYYYYMMDD(opd) = {part}
                        GROUP BY c, t, opd
                    """
                    _ch_exec(insert_sql, settings=ch_dedup_settings,
                             label=f"insert dedup part {part}", stage="insert_dedup", part=part, journal_pub=journal_pub)

                    _ch_exec(f"ALTER TABLE {ch_table_clean} REPLACE PARTITION {part} FROM {ch_table_buf}",
                             label=f"replace clean part {part}", stage="replace_clean", part=part, journal_pub=journal_pub)

                    _ch_exec(f"ALTER TABLE {ch_table_buf} DROP PARTITION {part}",
                             label=f"cleanup buf partition {part}", stage="drop_buf_cleanup", part=part, journal_pub=journal_pub)

                    part_ms = int((perf_counter() - part_t0) * 1000)
                    log.info("Дедуп: part=%s total %d мс", part, part_ms)
                    if journal_pub is not None:
                        try:
                            journal_pub.heartbeat(progress={"dedup_part_total_ms": {str(part): part_ms}})
                        except Exception as he:
                            log.debug("journal heartbeat (dedup_part_total_ms) не записан: %s", he)
                    log.info("Партиция %s: опубликована в %s.", part, ch_table_clean)
                    if publish_only_if_new:
                        total_after = None
                        try:
                            if "parts_raw_count" in locals():
                                total_after = parts_raw_count.get(part)  # type: ignore[name-defined]
                        except Exception:
                            total_after = None
                        if total_after is None:
                            total_after = _raw_count_for_part(part)
                        if total_after is not None:
                            _last_published_raw_count[part] = int(total_after)
                finally:
                    pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))
            if journal_pub is not None:
                try:
                    journal_pub.mark_done(pub_since, pub_until, rows_read=0, rows_written=0)
                except Exception as e:
                    log.debug("Не удалось завершить запись под-процесса дедупа: %s", e)
        except KeyboardInterrupt:
            if journal_pub is not None:
                try:
                    if hasattr(journal_pub, "mark_cancelled"):
                        journal_pub.mark_cancelled(pub_since, pub_until, message=_interrupt_message())
                    else:
                        journal_pub.mark_error(pub_since, pub_until, message=_interrupt_message())
                except Exception:
                    pass
            log.warning("Публикация прервана пользователем (Ctrl+C).")
            raise

        slices_since_last_pub = 0

    def _maybe_publish_after_slice() -> None:
        """Публикуем строго по числу успешных слайсов (таймер по минутам отключён)."""
        if publish_every_slices > 0 and slices_since_last_pub >= publish_every_slices:
            _publish_parts(pending_parts)
            pending_parts.clear()

    try:
        with journal.exclusive_lock() as got:
            if not got:
                log.warning("Другой инстанс '%s' уже выполняется — выходим.", process_name)
                return

            journal.sanitize_stale(
                planned_ttl_minutes=60,
                running_heartbeat_timeout_minutes=45,
                running_hard_ttl_hours=12,
            )

            logged_tz_context = False
            is_first_slice = True
            for s, e in iter_slices(since_dt, until_dt, step_min):
                _check_stop()
                s_q_base = s
                e_q_base = e

                use_overlap = overlap_delta_c if (is_first_slice or not overlap_only_first_slice) else timedelta(0)
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

                # лог окна
                if use_overlap > timedelta(0):
                    log.info(
                        "Слайс (бизнес): %s → %s | Phoenix: %s → %s | overlap=%d мин",
                        s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
                        int(use_overlap.total_seconds() // 60),
                    )
                else:
                    log.info(
                        "Слайс (бизнес): %s → %s | Phoenix: %s → %s",
                        s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
                    )

                try:
                    journal.clear_conflicting_planned(s, e)
                except Exception:
                    pass
                try:
                    journal.mark_planned(s, e)
                except UniqueViolation:
                    journal.clear_conflicting_planned(s, e)
                    journal.mark_planned(s, e)

                run_id = journal.mark_running(s, e, host=host, pid=pid)

                rows_read = 0
                ch_rows: List[tuple] = []

                try:
                    s_q_phx = s_q.replace(tzinfo=None) if s_q.tzinfo else s_q
                    e_q_phx = e_q.replace(tzinfo=None) if e_q.tzinfo else e_q

                    for batch in phx.fetch_increment(cfg.HBASE_MAIN_TABLE, cfg.HBASE_MAIN_TS_COLUMN, effective, s_q_phx, e_q_phx):
                        _check_stop()
                        rows_read += len(batch)
                        for r in batch:
                            ch_rows.append(_row_to_ch_tuple(r))

                        if len(ch_rows) >= ch_batch:
                            total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                            ch_rows.clear()
                            journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})

                    if ch_rows:
                        total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                        ch_rows.clear()

                    # накопим партиции текущего слайда — СТРОГО ПО UTC
                    for p in _iter_partitions_by_day_tz(s_q, e_q, "UTC"):
                        pending_parts.add(p)
                    slices_since_last_pub += 1

                    is_first_slice = False

                    journal.mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
                    total_read += rows_read

                    try:
                        _pp = ",".join(str(p) for p in sorted(pending_parts)) or "-"
                        log.info("Слайс завершён: rows_read=%d, rows_written_raw_total=%d, pending_parts=%s",
                                 rows_read, total_written_ch, _pp)
                    except Exception:
                        pass

                    _maybe_publish_after_slice()

                except KeyboardInterrupt:
                    try:
                        if hasattr(journal, "mark_cancelled"):
                            journal.mark_cancelled(s, e, message=_interrupt_message())
                        else:
                            journal.mark_error(s, e, message=_interrupt_message())
                    except Exception:
                        pass
                    raise
                except Exception as ex:
                    journal.heartbeat(run_id, progress={"error": str(ex)})
                    journal.mark_error(s, e, message=str(ex))
                    raise

            if always_publish_at_end and pending_parts:
                log.info("Финальная публикация: %d партиций.", len(pending_parts))
                _publish_parts(pending_parts, force=True)
                pending_parts.clear()


        log.info("Готово. Прочитано: %d | в CH записано: %d", total_read, total_written_ch)

    finally:
        try:
            phx.close()
        except Exception:
            pass
        try:
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
            _log.error(_msg + " (stack suppressed; set ETL_TRACE_EXC=1 или --log-level=DEBUG for traceback)")
        raise SystemExit(1)
    