# file: scripts/codes_history_etl.py
# -*- coding: utf-8 -*-
"""
Incremental ETL: Phoenix(HBase) → ClickHouse (только Native 9000, c компрессией).
Требования:
- переносим поля «как есть» (никаких sys-полей и вычислений id/md5),
- дедуп в рамках запуска по (c, t, opd),
- окно запроса к Phoenix сдвигаем относительно бизнес-интервала (PHX_QUERY_SHIFT_MINUTES),
- DateTime64(3) передаём как python datetime (naive), обрезая до миллисекунд,
- никаких CSV: сразу вставляем в ClickHouse.

Чтобы избежать ошибки clickhouse-driver вида "'str' has no attribute 'tzinfo'",
строго передаём DateTime в виде datetime, а не строк.
"""
from __future__ import annotations
import argparse
import logging
import os
import re
import json
import socket
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from .logging_setup import setup_logging
from .config import Settings, parse_iso_utc
from .slicer import iter_slices
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal

log = logging.getLogger("codes_history_etl")

# Порядок колонок должен 1-в-1 совпадать с DDL stg.daily_codes_history
CH_COLUMNS = [
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
]

DT_FIELDS = {"opd","emd","apd","exd","tm"}
INT8_FIELDS  = {"t","st","ste","elr","pt","et"}
INT16_FIELDS = {"pg"}
INT64_FIELDS = {"tt"}  # при необходимости добавляй сюда

def _to_dt64_obj(v: Any) -> Any:
    """
    Любое значение → datetime (naive) с точностью до миллисекунд, либо None.
    Если пришёл aware — TZ выбрасываем, момент не сдвигаем.
    """
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip()
        if not s:
            return None
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
    ch → Array(String): возвращаем [] если пусто, иначе массив строк.
    Поддерживаем list/tuple/JSON-строку/"A,B;C".
    """
    if v is None or v == "" or v == "{}" or v == "[]":
        return []
    if isinstance(v, (list, tuple)):
        arr = [str(x).strip() for x in v if x is not None and str(x).strip() != ""]
        return arr or []
    s = str(v).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            arr = [str(x).strip() for x in parsed if x is not None and str(x).strip() != ""]
            return arr or []
    except Exception:
        pass
    raw = s.strip("[]{}()")
    arr = [p.strip() for p in re.split(r"[,;|]", raw) if p.strip()]
    return arr or []

def _row_to_ch_tuple(r: Dict[str, Any]) -> tuple:
    """
    Приведение типов под ClickHouse DDL (см. CH_COLUMNS).
    """
    out = []
    for col in CH_COLUMNS:
        val = r.get(col)
        if col in DT_FIELDS:
            out.append(_to_dt64_obj(val))
        elif col in INT8_FIELDS or col in INT16_FIELDS or col in INT64_FIELDS:
            out.append(_as_int(val))
        elif col == "ch":
            out.append(_parse_ch_storage(val))  # теперь гарантированно [] либо список строк
        else:
            out.append(val if val not in ("",) else None)
    return tuple(out)

def main():
    parser = argparse.ArgumentParser(description="Codes History Incremental (Phoenix→ClickHouse)")
    parser.add_argument("--since", required=False, help="ISO (UTC) или без TZ (считаем UTC). Пр: 2025-08-08T00:00:00")
    parser.add_argument("--until", required=True,  help="ISO (UTC). Пр: 2025-08-09T00:00:00")
    parser.add_argument("--step-min", type=int, default=None, help="Размер слайда (мин). По умолчанию из .env STEP_MIN")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = Settings()

    # Журнал и коннекты
    pg = PGClient(cfg.PG_DSN)
    journal = ProcessJournal(pg, cfg.JOURNAL_TABLE, cfg.PROCESS_NAME)
    journal.ensure()

    phx = PhoenixClient(cfg.PQS_URL, fetchmany_size=cfg.PHX_FETCHMANY_SIZE, ts_units=cfg.PHX_TS_UNITS)
    ch  = CHClient(
        hosts=cfg.ch_hosts_list(),
        port=cfg.CH_PORT,
        database=cfg.CH_DB,
        user=cfg.CH_USER,
        password=cfg.CH_PASSWORD,
        compression=True,  # native+compression → нужен clickhouse-cityhash
    )
    ch_table = cfg.CH_TABLE
    ch_batch = int(cfg.CH_INSERT_BATCH)

    # Времена
    since_dt = parse_iso_utc(args.since) if args.since else None
    until_dt = parse_iso_utc(args.until)
    step_min = args.step_min if args.step_min is not None else int(cfg.STEP_MIN)
    qshift   = timedelta(minutes=int(cfg.PHX_QUERY_SHIFT_MINUTES))

    # Схема Phoenix → валидация списка колонок
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

    # Watermark
    watermark = journal.get_watermark()
    if since_dt is None:
        if watermark is not None:
            since_dt = watermark
            log.info("Старт от watermark (PG, UTC): %s", since_dt.isoformat())
        else:
            raise SystemExit("Не задан --since и нет watermark в журнале")

    total_read = 0
    total_written_ch = 0
    host = socket.gethostname()
    pid = os.getpid()

    # Множество ключей, встреченных в ЭТОМ запуске (дедуп ETL по (c,t,opd))
    seen_keys = set()

    try:
        with journal.exclusive_lock() as got:
            if not got:
                log.warning("Другой инстанс '%s' уже выполняется — выходим.", cfg.PROCESS_NAME)
                return

            # Подчистим возможные "висяки" перед планированием новых срезов:
            # - planned старше 60 мин → skipped
            # - running без heartbeat &gt; 45 мин → error
            # - running старше 12 часов (жёсткий TTL) → error
            journal.sanitize_stale(
                planned_ttl_minutes=60,
                running_heartbeat_timeout_minutes=45,
                running_hard_ttl_hours=12,
            )

            for s, e in iter_slices(since_dt, until_dt, step_min):
                s_q = s + qshift
                e_q = e + qshift
                log.info("Слайс (бизнес): %s → %s | запрос к Phoenix: %s → %s | shift=%+d мин",
                         s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(), int(cfg.PHX_QUERY_SHIFT_MINUTES))
                journal.mark_planned(s, e)
                run_id = journal.mark_running(s, e, host=host, pid=pid)

                rows_read = 0
                ch_rows: List[tuple] = []

                try:
                    # Phoenix expects offset-naive datetime for TIMESTAMP; strip tzinfo from UTC-aware values
                    s_q_phx = s_q.replace(tzinfo=None) if s_q.tzinfo else s_q
                    e_q_phx = e_q.replace(tzinfo=None) if e_q.tzinfo else e_q
                    for batch in phx.fetch_increment(cfg.HBASE_MAIN_TABLE, cfg.HBASE_MAIN_TS_COLUMN, effective, s_q_phx, e_q_phx):
                        rows_read += len(batch)

                        for r in batch:
                            # Дедуп ключ из нормализованных значений
                            key = (
                                r.get("c"),
                                _as_int(r.get("t")),
                                _to_dt64_obj(r.get("opd")),
                            )
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)

                            ch_rows.append(_row_to_ch_tuple(r))

                        # флаш по батчу
                        if len(ch_rows) >= ch_batch:
                            total_written_ch += ch.insert_rows(ch_table, ch_rows, CH_COLUMNS)
                            ch_rows.clear()
                            journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})

                    # хвост
                    if ch_rows:
                        total_written_ch += ch.insert_rows(ch_table, ch_rows, CH_COLUMNS)
                        ch_rows.clear()

                    journal.mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
                    total_read += rows_read

                except Exception as ex:
                    journal.heartbeat(run_id, progress={"error": str(ex)})
                    journal.mark_error(s, e, message=str(ex))
                    raise

        # Ретеншн журнала (если включён)
        if int(getattr(cfg, "JOURNAL_RETENTION_DAYS", 0) or 0) > 0:
            # простая очистка завершённых записей старше N дней (необязательная)
            pass

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
    main()