# -*- coding: utf-8 -*-
"""
Incremental ETL: Phoenix(HBase) → ClickHouse (+ PG-журнал, CSV опционально).

Что важно именно в этой версии:
- DateTime из источника храним «как есть» в ClickHouse (DateTime64(3)), без перевода TZ.
  * Для строк -> передаём строку "YYYY-MM-DD HH:MM:SS.mmm".
  * Для datetime -> форматируем в строку с миллисекундами (отрезаем до .mmm).
- Окно запроса в Phoenix сдвигаем на -5 часов по умолчанию (PHX_QUERY_SHIFT_MINUTES=-300).
- Поле ch храним как Array(String) (оригинальный порядок); для ключа уникальности канонизируем (distinct+sort).
- Жёсткая дедупликация на уровне ETL за весь запуск процесса (--since..--until..):
  дубликаты по бизнес-ключу (без служебки) не вставляются.
- Служебка ts/q/ts_ingested/etl_job/load_id оставлена: полезна для анализа, аудита, SLA.
"""

import os
import re
import csv
import json
import argparse
import logging
import time
import socket
import hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Any, Dict

from .logging_setup import setup_logging
from .config import Settings, parse_iso_utc
from .slicer import iter_slices
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal

log = logging.getLogger("codes_history_etl")

# Если .env не задали список колонок — используем этот базовый
ALL_COLUMNS: List[str] = [
    "c", "t", "opd", "tm",
    "id", "did",
    "rid", "rinn", "rn", "sid", "sinn", "sn",
    "gt", "prid", "pg", "pn", "b",
    "st", "ste", "elr", "et",
    "emd", "apd", "exd",
    "p", "pt", "ch",
    "o", "tt", "j", "ag",
    "pvad",
]

HEARTBEAT_EVERY_SEC = 10

# Поля, которые НЕ участвуют в ключе уникальности (служебка/искусственные)
NON_BUSINESS_FIELDS = {"ts", "q", "ts_ingested", "etl_job", "load_id", "id"}  # 'id' искусственный

# ----------------- Работа со схемой Phoenix -----------------
def _discover_columns_via_limit0(pqs_url: str, table: str) -> List[str]:
    try:
        import phoenixdb
    except Exception as e:
        raise RuntimeError("Не удалось импортировать phoenixdb (нужен для чтения схемы).") from e
    conn = phoenixdb.connect(pqs_url, autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{table}" LIMIT 0')
        return [d[0] for d in (cur.description or [])]
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _resolve_effective_columns(requested: List[str], available: List[str]) -> Tuple[List[str], List[str]]:
    avail_map = {a.lower(): a for a in available}
    effective, missing = [], []
    for c in requested:
        k = c.strip('"').strip().lower()
        if not k:
            continue
        if k in avail_map:
            effective.append(avail_map[k])
        else:
            missing.append(c)
    return effective, missing

def _columns_from_settings_or_fallback(cfg: Settings) -> List[str]:
    try:
        cols = cfg.main_columns_list()
    except Exception:
        cols = []
    if not cols or len(cols) < 3:
        cols = ALL_COLUMNS
    return cols

# ----------------- Утилиты конфигурации -----------------
def _cfg_value(cfg: Settings, name: str, default=None):
    v = getattr(cfg, name, default)
    if isinstance(v, str):
        v = v.split("#", 1)[0].strip()
        if v == "":
            return default
    return v

def _cfg_bool(cfg: Settings, name: str, default: bool = False) -> bool:
    v = _cfg_value(cfg, name, None)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _cfg_int(cfg: Settings, name: str, default: int = 0) -> int:
    v = _cfg_value(cfg, name, default)
    try:
        return int(v)
    except Exception:
        return default

# ----------------- CSV writer -----------------
def write_csv(export_dir: str, export_prefix: str, start: datetime, end: datetime,
              rows: List[dict], columns: List[str]) -> int:
    os.makedirs(export_dir, exist_ok=True)
    sf = start.strftime("%Y%m%d_%H%M%S")
    ef = end.strftime("%Y%m%d_%H%M%S")
    path = os.path.join(export_dir, f"{export_prefix}{sf}__{ef}.csv")
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c) for c in columns})
    return len(rows)

# ----------------- Приведение типов к CH -----------------
# ВАЖНО: порядок должен совпадать с DDL stg.daily_codes_history (кроме MATERIALIZED/DEFAULT)
CH_COLUMNS = [
    "c","t","opd","id","did",
    "rid","rinn","rn","sid","sinn","sn",
    "gt","prid","st","ste","elr",
    "emd","apd","exd",
    "p","pt","o","pn","b","tt","tm","ch","j",
    "pg","et","pvad","ag",
    "ts","q"
]

DT_FIELDS = {"opd","emd","apd","exd","tm","ts"}
INT8_FIELDS  = {"t","st","ste","elr","pt","et","q"}
INT16_FIELDS = {"pg"}
INT64_FIELDS = {"tt"}  # добавляй сюда при необходимости

def _as_dt_str_keep(v: Any) -> Any:
    """
    Возвращаем строку DateTime64(3) «как есть», чтобы CH записал точность до миллисекунд.
    - None / "" -> None
    - datetime -> "YYYY-MM-DD HH:MM:SS.mmm"
    - иначе -> str(v) (если источник уже отдаёт строку в нужном формате)
    """
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        # форматируем до миллисекунд
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    s = str(v).strip()
    # если пришла строка без мс — оставим как есть; CH сам примет её
    return s if s else None

def _as_int(v: Any) -> Any:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None

def _parse_ch_array_storage(v: Any) -> List[str]:
    """
    Преобразуем значение поля ch к Array(String) для ХРАНЕНИЯ (оригинальный порядок).
    - list/tuple → приводим элементы к str, фильтруем пустые
    - str(JSON) → если это JSON list → как list; иначе пытаемся распарсить "A,B;C" / "{A,B}"
    """
    if v is None or v == "" or v == "{}":
        return []
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if x is not None and str(x).strip() != ""]
    s = str(v).strip()
    if not s:
        return []
    # сначала пробуем как JSON
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if x is not None and str(x).strip() != ""]
    except Exception:
        pass
    # затем — просто разделители
    raw = s.strip("[]{}()")
    return [p.strip() for p in re.split(r"[,;|]", raw) if p.strip()]

def _canon_ch_for_key(v: Any) -> tuple:
    """
    Канонизация ch для КЛЮЧА: distinct + sort (порядок не влияет на уникальность).
    """
    return tuple(sorted(set(_parse_ch_array_storage(v))))

def _norm_for_key(col: str, val: Any) -> Any:
    """
    Нормализуем значение для ключа уникальности:
    - DT → строка ISO с миллисекундами (UTC или «как есть», но стабильный формат)
    - ch → канонизированный tuple
    - list/dict → каноничный JSON
    - остальное → str().strip()
    """
    if val is None or val == "":
        return None
    if col in DT_FIELDS:
        # пробуем аккуратно распарсить; если не получилось — оставим str
        try:
            dt = parse_iso_utc(str(val))
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except Exception:
            if isinstance(val, datetime):
                return val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return str(val).strip()
    if col == "ch":
        return _canon_ch_for_key(val)
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return str(val).strip()

def _row_to_ch_tuple(r: Dict[str, Any]) -> tuple:
    """
    Готовим к вставке в ClickHouse: порядок и типы соответствуют CH_COLUMNS.
    - DT → строка (с мс) через _as_dt_str_keep
    - ch → Array(String) через _parse_ch_array_storage
    """
    out = []
    for col in CH_COLUMNS:
        val = r.get(col)
        if col in DT_FIELDS:
            out.append(_as_dt_str_keep(val))
        elif col in INT8_FIELDS or col in INT16_FIELDS or col in INT64_FIELDS:
            out.append(_as_int(val))
        elif col == "ch":
            out.append(_parse_ch_array_storage(val))
        else:
            # пустую строку превращаем в NULL
            out.append(val if val not in ("",) else None)
    return tuple(out)

# ----------------------------------- main -----------------------------------
def main():
    parser = argparse.ArgumentParser(description="Codes History Incremental (HBase→CH/CSV + PG)")
    parser.add_argument("--since", required=False, help="ISO без TZ ОК. Пример: 2025-01-15T00:00:00")
    parser.add_argument("--until", required=True, help="ISO. Пример: 2025-01-16T00:00:00")
    parser.add_argument("--step-min", type=int, default=None, help="Размер слайса (мин). По умолчанию из env.")

    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--migrate-only", action="store_true", help="Только применить миграции PG и выйти")
    parser.add_argument("--query-shift-min", type=int, default=None,
                        help="Сдвиг окна к Phoenix в минутах (например, -300). "
                             "По умолчанию PHX_QUERY_SHIFT_MINUTES или -300.")

    parser.add_argument("--list-columns", action="store_true", help="Показать фактические колонки Phoenix и выйти.")
    parser.add_argument("--columns-strict", action="store_true", help="Если колонок из .env нет — аварийно завершить.")
    parser.add_argument("--sys-fields", action="store_true", help="Явно включить системные поля (перекроет .env).")
    parser.add_argument("--no-sys-fields", action="store_true", help="Явно выключить системные поля (перекроет .env).")

    parser.add_argument("--sink", choices=("clickhouse","csv","both"), default=None,
                        help="Куда писать данные (дефолт из .env SINK, по умолчанию clickhouse)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = Settings()

    # --list-columns
    if args.list_columns:
        try:
            cols = _discover_columns_via_limit0(cfg.PQS_URL, cfg.HBASE_MAIN_TABLE)
        except Exception as e:
            log.exception("Не удалось получить список колонок у Phoenix: %s", e)
            raise SystemExit(2)
        print(f"Phoenix columns for table {cfg.HBASE_MAIN_TABLE}:")
        for c in cols:
            print(" -", c)
        print("\n.env → HBASE_MAIN_COLUMNS=" + ",".join(cols))
        return

    # режимы
    columns_strict = args.columns_strict or _cfg_bool(cfg, "PHX_COLUMNS_STRICT")

    # системные поля: дефолт ON; приоритет CLI > .env
    if args.no_sys_fields:
        add_sys_fields = False
    elif args.sys_fields:
        add_sys_fields = True
    else:
        add_sys_fields = _cfg_bool(cfg, "ETL_ADD_SYS_FIELDS", True)
    sys_ts_name_raw = (_cfg_value(cfg, "ETL_SYS_TS_NAME", "ts") or "ts").strip()
    sys_id_name_raw = (_cfg_value(cfg, "ETL_SYS_ID_NAME", "id") or "id").strip()

    # sink
    sink = args.sink or (_cfg_value(cfg, "SINK", "clickhouse") or "clickhouse").lower()

    # сдвиг окна к Phoenix
    if args.query_shift_min is not None:
        qshift_min = args.query_shift_min
    else:
        # по умолчанию -5 часов, как ты просил
        qshift_min = _cfg_int(cfg, "PHX_QUERY_SHIFT_MINUTES", -300)
    qshift = timedelta(minutes=qshift_min)

    # коннекты
    phx = PhoenixClient(cfg.PQS_URL, fetchmany_size=cfg.PHX_FETCHMANY_SIZE, ts_units=cfg.PHX_TS_UNITS)
    pg = PGClient(cfg.PG_DSN)
    journal = ProcessJournal(pg, cfg.JOURNAL_TABLE, cfg.PROCESS_NAME)

    # ClickHouse (по требованию sink)
    ch = None
    if sink in ("clickhouse","both") and not args.dry_run:
        ch = CHClient(
            hosts=cfg.ch_hosts_list(),
            database=cfg.CH_DB,
            user=cfg.CH_USER,
            password=cfg.CH_PASSWORD,
            port=cfg.CH_PORT,
            protocol=(_cfg_value(cfg, "CH_PROTOCOL", "auto") or "auto"),
            compression=False,  # важно: чтобы не требовался clickhouse-cityhash
        )
        ch_table = cfg.CH_TABLE
        ch_batch = int(cfg.CH_INSERT_BATCH)

    # журнал
    journal.ensure()
    if args.migrate_only:
        journal.apply_sql_migrations()
        log.info("Миграции применены. Выходим (--migrate-only).")
        phx.close(); pg.close()
        return

    since_dt = parse_iso_utc(args.since) if args.since else None
    until_dt = parse_iso_utc(args.until)

    # эксклюзивный запуск
    with journal.exclusive_lock() as got:
        if not got:
            log.warning("Другой инстанс '%s' уже выполняется — выходим.", cfg.PROCESS_NAME)
            phx.close(); pg.close()
            return

        # санация
        journal.sanitize_stale(
            planned_ttl_minutes=60,
            running_heartbeat_timeout_minutes=45,
            running_hard_ttl_hours=12
        )

        # seed watermark
        if journal.get_watermark() is None:
            mx = phx.max_ts_from_inc_processing('WORK.INC_PROCESSING', 'PROCESS', 'TS', cfg.PROCESS_NAME)
            if mx is None:
                mx = phx.max_ts_from_inc_processing('WORK.INC_PROCESSING', ts_col='TS')
            journal.seed_from_hbase_max_ts(mx)

        watermark = journal.get_watermark()
        if since_dt is None:
            if watermark is not None:
                since_dt = watermark
                log.info("Старт от watermark (PG, UTC): %s", since_dt.isoformat())
            else:
                raise SystemExit("Не задан --since и нет watermark в журнале")

        step_min = args.step_min if args.step_min is not None else _cfg_int(cfg, "STEP_MIN", 60)

        # согласование колонок
        requested_columns = _columns_from_settings_or_fallback(cfg)
        try:
            available_columns = _discover_columns_via_limit0(cfg.PQS_URL, cfg.HBASE_MAIN_TABLE)
        except Exception as e:
            log.exception("Не удалось прочитать схему Phoenix: %s", e)
            raise SystemExit(2)

        ts_col = cfg.HBASE_MAIN_TS_COLUMN
        avail_lower = {c.lower(): c for c in available_columns}
        if ts_col.strip().lower() not in avail_lower:
            log.error("Колонка таймстемпа '%s' отсутствует в '%s'. Доступно: %s",
                      ts_col, cfg.HBASE_MAIN_TABLE, ", ".join(available_columns))
            raise SystemExit(2)

        effective_columns, missing = _resolve_effective_columns(requested_columns, available_columns)
        if missing:
            miss_str = ", ".join(missing)
            if columns_strict:
                log.error("Строгий режим: отсутствуют колонки в Phoenix: %s", miss_str)
                raise SystemExit(2)
            else:
                log.warning("Некоторые колонки из .env отсутствуют и будут пропущены: %s", miss_str)
        if not effective_columns:
            log.error("После согласования колонок не осталось ни одной для SELECT — выход.")
            raise SystemExit(2)

        # системные поля: коллизии имён
        log.info("SYS-FIELDS: %s (ts=%r, id=%r)", "ON" if add_sys_fields else "OFF", sys_ts_name_raw, sys_id_name_raw)
        used_sys_ts_name = sys_ts_name_raw
        used_sys_id_name = sys_id_name_raw

        lc = {c.lower() for c in effective_columns}
        if add_sys_fields:
            if used_sys_ts_name.lower() in lc:
                new_name = f"{used_sys_ts_name}_sys"
                log.warning("SYS-FIELDS: имя %r уже занято в источнике → используем %r",
                            used_sys_ts_name, new_name)
                used_sys_ts_name = new_name
            if used_sys_id_name.lower() in lc:
                log.warning("SYS-FIELDS: колонка %r присутствует в источнике; "
                            "она будет перезаписана md5(did) или NULL.", used_sys_id_name)

        # ensure did для md5(id)
        compute_md5_id = False
        did_key = None
        if add_sys_fields:
            if "did" in lc:
                did_key = next(c for c in effective_columns if c.lower() == "did")
                compute_md5_id = True
            elif "did" in avail_lower:
                did_key = avail_lower["did"]
                effective_columns.append(did_key)
                compute_md5_id = True
                log.info("Для расчёта id=md5(did) автоматически добавлена колонка '%s' в SELECT.", did_key)
            else:
                log.warning("Колонка 'did' отсутствует в источнике — sys id будет NULL.")

        # CSV список
        csv_columns = list(effective_columns)
        if add_sys_fields:
            if used_sys_ts_name.lower() not in {c.lower() for c in csv_columns}:
                csv_columns.append(used_sys_ts_name)
            if used_sys_id_name.lower() not in {c.lower() for c in csv_columns}:
                csv_columns.append(used_sys_id_name)

        log.info("Итоговые колонки для SELECT (%d): %s", len(effective_columns), ", ".join(effective_columns))
        log.info("CSV колонки (%d): %s", len(csv_columns), ", ".join(csv_columns))

        table = cfg.HBASE_MAIN_TABLE
        export_prefix = getattr(cfg, "EXPORT_PREFIX", "codes_history_")

        total_read = 0
        total_written_csv = 0
        total_written_ch = 0
        skipped_dups_total = 0
        host = socket.gethostname()
        pid = os.getpid()

        # Множество ключей, встреченных в ЭТОМ запуске (дедуп ETL)
        seen_keys = set()

        # основной цикл
        for s, e in iter_slices(since_dt, until_dt, step_min):
            s_q = s + qshift
            e_q = e + qshift

            log.info("Слайс (бизнес): %s → %s | запрос к Phoenix: %s → %s | shift=%+d мин",
                     s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(), qshift_min)

            journal.mark_planned(s, e)
            run_id = journal.mark_running(s, e, host=host, pid=pid)

            read_count = 0
            csv_buffer: List[dict] = []
            ch_rows: List[tuple] = []
            last_hb = time.monotonic()

            try:
                for batch in phx.fetch_increment(table, ts_col, effective_columns, s_q, e_q):
                    read_count += len(batch)

                    # подготовка полей
                    # ts = верх окна e (формируем строку с мс)
                    ts_str = e.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                    # сформируем список колонок для ключа (без служебки)
                    nonbiz = {x.lower() for x in NON_BUSINESS_FIELDS}
                    key_cols = [c for c in effective_columns if c.lower() not in nonbiz]

                    for r in batch:
                        if add_sys_fields:
                            r[used_sys_ts_name] = ts_str
                            # id = md5(did) | NULL
                            if compute_md5_id and did_key:
                                did_val = r.get(did_key)
                                if did_val is not None and str(did_val) != "":
                                    new_id = hashlib.md5(str(did_val).encode("utf-8")).hexdigest()
                                    r[used_sys_id_name] = new_id
                                else:
                                    r[used_sys_id_name] = None
                            else:
                                r[used_sys_id_name] = None

                        # Для CH имена должны быть 'ts' и 'id' (как в DDL), даже если sys-имена иные
                        if used_sys_ts_name != "ts":
                            r["ts"] = r.get(used_sys_ts_name)
                        if used_sys_id_name != "id":
                            r["id"] = r.get(used_sys_id_name)

                        # флаг q (если не приходит из источника)
                        r.setdefault("q", 0)

                        # ====== ДЕДУПЛИКАЦИЯ на уровне ETL (за весь запуск) ======
                        dedup_key = tuple(_norm_for_key(col, r.get(col)) for col in key_cols)
                        if dedup_key in seen_keys:
                            skipped_dups_total += 1
                            continue
                        seen_keys.add(dedup_key)
                        # =========================================================

                        # CSV-буфер
                        if sink in ("csv", "both"):
                            csv_buffer.append(r)

                        # CH-буфер
                        if sink in ("clickhouse", "both"):
                            ch_rows.append(_row_to_ch_tuple(r))

                    # heartbeat
                    now = time.monotonic()
                    if now - last_hb >= HEARTBEAT_EVERY_SEC:
                        journal.heartbeat(run_id, progress={"rows_read": read_count, "dups_skipped": skipped_dups_total})
                        last_hb = now

                    # батчи в CH
                    if ch and len(ch_rows) >= cfg.CH_INSERT_BATCH:
                        total_written_ch += ch.insert_rows(ch_table, ch_rows, CH_COLUMNS)
                        ch_rows.clear()

                # хвостовые записи
                if ch and ch_rows:
                    total_written_ch += ch.insert_rows(ch_table, ch_rows, CH_COLUMNS)
                    ch_rows.clear()

                if sink in ("csv", "both") and not args.dry_run:
                    written = write_csv(cfg.EXPORT_DIR, export_prefix, s, e, csv_buffer, csv_columns)
                    total_written_csv += written
                    log.info("CSV: записано %d строк", written)

                journal.mark_done(
                    s, e,
                    rows_read=read_count,
                    rows_written=(len(csv_buffer) if sink in ("csv","both") else total_written_ch),
                    message=f"dups_skipped={skipped_dups_total}"
                )
                total_read += read_count

            except Exception as ex:
                journal.heartbeat(run_id, progress={"error": str(ex)})
                journal.mark_error(s, e, message=str(ex))
                raise

        # ретеншн журнала
        if getattr(cfg, "JOURNAL_RETENTION_DAYS", 0) and cfg.JOURNAL_RETENTION_DAYS > 0:
            journal.purge_older_than_days(cfg.JOURNAL_RETENTION_DAYS)

        log.info("Готово. Прочитано: %d | в CH записано: %d | в CSV записано: %d | дублей пропущено: %d",
                 total_read, total_written_ch, total_written_csv, skipped_dups_total)

    phx.close()
    pg.close()

if __name__ == "__main__":
    main()