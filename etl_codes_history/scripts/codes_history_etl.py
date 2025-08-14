# file: scripts/codes_history_etl.py
# -*- coding: utf-8 -*-
"""
Incremental ETL: Phoenix(HBase) → ClickHouse (только Native 9000, c компрессией).

Ключевые принципы:
- Переносим поля «как есть» (никаких sys-полей и вычислений id/md5).
- Дедуп в рамках публикации по (c, t, opd) через argMax(..., ingested_at).
- Окно запроса к Phoenix: бизнес-интервал + сдвиг PHX_QUERY_SHIFT_MINUTES
  + «захлёст» назад PHX_QUERY_OVERLAP_MINUTES (обычно только на первом слайсе)
  + «лаг» справа PHX_QUERY_LAG_MINUTES (не трогаем «кипящее»).
- DateTime64(3) передаём как python datetime (naive), обрезая до миллисекунд.
- Никаких CSV: сразу INSERT в ClickHouse (native 9000, compression).
- Публикация (дедуп/REPLACE) делается **автоматически по каденсу**, без ручных флагов:
  • PUBLISH_EVERY_MINUTES — минимальный интервал времени между публикациями,
  • PUBLISH_EVERY_SLICES  — минимальное число успешно обработанных слайсов.
  По достижении одного из порогов выполняется публикация «накопленных» партиций
  и скрипт продолжает обработку дальше вплоть до правой границы --until.
  Для надёжности можно включить ALWAYS_PUBLISH_AT_END=1 (по умолчанию) — финальная публикация в конце запуска.

Чтобы избежать ошибки clickhouse-driver вида "'str' has no attribute 'tzinfo'",
строго передаём DateTime в виде datetime, а не строк.

────────────────────────────────────────────────────────────────────────────────
КАК ЭТО РАБОТАЕТ (сквозной поток)
1) Разбиваем заданное окно [--since, --until) на «слайды» длиной STEP_MIN (например 10–60 минут).
   Для каждого бизнес-слайда вычисляется окно запроса в Phoenix с учётом:
   shift (PHX_QUERY_SHIFT_MINUTES), overlap (PHX_QUERY_OVERLAP_MINUTES) и lag (PHX_QUERY_LAG_MINUTES).

2) Для КАЖДОГО слайда:
   • В журнале PG фиксируем planned → running (ProcessJournal), обновляем heartbeat.
   • Читаем Phoenix порциями (fetchmany PHX_FETCHMANY_SIZE), упорядочено по времени.
   • Для каждой строки:
       - нормализуем типы под ClickHouse (TZ → naive, microseconds → milliseconds),
       - локально дедупим ключ (c, t, opd) в рамках ТЕКУЩЕГО запуска,
       - кладём кортеж в буфер ch_rows.
   • Как только буфер достиг CH_INSERT_BATCH (например 20000) — выполняем INSERT VALUES в RAW.
     Хвост < порога — отдельный INSERT в конце слайда.
   • Запоминаем партиции (toYYYYMMDD(opd)), которых коснулся текущий слайс.

3) Автопубликация (внутри основного цикла):
   • Если прошёл порог по времени (PUBLISH_EVERY_MINUTES) и/или набралось
     достаточно удачных слайсов (PUBLISH_EVERY_SLICES), выполняем публикацию:
     DROP BUF → INSERT BUF (GROUP BY ... argMax) → REPLACE CLEAN → DROP BUF.
     Операции измеряются (perf_counter) и логируются. Заводим отдельную запись в журнале
     с именем '<PROCESS_NAME>:dedup' на каждую публикацию.

4) По завершении окна (или если включён ALWAYS_PUBLISH_AT_END) — финальная публикация
   оставшихся партиций (если есть).

5) Надёжность:
   • Один активный инстанс процесса (advisory-lock + частичный UNIQUE-индекс).
   • Санитация planned/running, heartbeat-таймауты и жёсткий TTL до старта.
   • CH-операции обёрнуты в мягкий retry при UnexpectedPacketFromServerError (EndOfStream):
     reconnect() + один повтор.

РЕЗЮМЕ ПО INSERT:
• Не копим сутки в памяти. Пишем в RAW по мере накопления буфера, «хвост» на каждом слайсе.
• Публикация (INSERT SELECT → REPLACE PARTITION) — периодическая/финальная, управляется каденсом.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import json
import socket
from psycopg.errors import UniqueViolation
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Iterable, Optional, Set

from .logging_setup import setup_logging
from .config import Settings, parse_iso_utc
from .slicer import iter_slices
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal
from clickhouse_driver import errors as ch_errors
from time import perf_counter

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
CH_COLUMNS_STR = ", ".join(CH_COLUMNS)

DT_FIELDS = {"opd","emd","apd","exd","tm"}
INT8_FIELDS  = {"t","st","ste","elr","pt","et"}
INT16_FIELDS = {"pg"}
INT64_FIELDS = {"tt"}  # при необходимости добавляй сюда

# ------------------------ УТИЛИТЫ ТИПИЗАЦИИ ------------------------

def _to_dt64_obj(v: Any) -> Any:
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
    out = []
    for col in CH_COLUMNS:
        val = r.get(col)
        if col in DT_FIELDS:
            out.append(_to_dt64_obj(val))
        elif col in INT8_FIELDS or col in INT16_FIELDS or col in INT64_FIELDS:
            out.append(_as_int(val))
        elif col == "ch":
            out.append(_parse_ch_storage(val))
        else:
            out.append(val if val not in ("",) else None)
    return tuple(out)

# ------------------------ ВСПОМОГАТЕЛЬНОЕ ------------------------

def _iter_partitions_by_day(start_dt: datetime, end_dt: datetime) -> Iterable[int]:
    """
    Возвращает toYYYYMMDD(int) для всех дней в полуинтервале [start_dt, end_dt).
    """
    if start_dt >= end_dt:
        return []
    start_day = start_dt.date()
    end_day_inclusive = (end_dt - timedelta(milliseconds=1)).date()
    d = start_day
    while d <= end_day_inclusive:
        yield int(d.strftime("%Y%m%d"))
        d += timedelta(days=1)

def _parts_to_interval(parts: Iterable[int]) -> Tuple[datetime, datetime]:
    """
    По множеству партиций строим (since, until) в UTC для под-журнала публикации.
    """
    ps = sorted(set(parts))
    if not ps:
        now = datetime.now(timezone.utc)
        return now, now
    d0 = datetime.strptime(str(ps[0]), "%Y%m%d").date()
    d1 = datetime.strptime(str(ps[-1]), "%Y%m%d").date()
    start = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
    end   = datetime(d1.year, d1.month, d1.day, tzinfo=timezone.utc) + timedelta(days=1)
    return start, end

# ------------------------ ОСНОВНОЙ СЦЕНАРИЙ ------------------------

def main():
    parser = argparse.ArgumentParser(description="Codes History Incremental (Phoenix→ClickHouse)")
    parser.add_argument("--since", required=False, help="ISO (UTC) или без TZ (считаем UTC). Пр: 2025-08-08T00:00:00")
    parser.add_argument("--until", required=True,  help="ISO (UTC). Пр: 2025-08-09T00:00:00")
    parser.add_argument("--step-min", type=int, default=None, help="Размер слайда (мин). По умолчанию из .env STEP_MIN")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--manual-start", action="store_true", help="Если указан, журнал ведётся под именем 'manual_<PROCESS_NAME>' (ручной запуск).")
    # Флаги оставлены как аварийные «оверрайды», но в обычной эксплуатации не требуются:
    parser.add_argument("--no-publish", action="store_true", help="Принудительно отключить публикацию (override автокаденса).")
    parser.add_argument("--force-publish", action="store_true", help="Принудительно выполнить публикацию (override автокаденса).")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = Settings()

    # Имя процесса в журнале
    process_name = ("manual_" + str(getattr(cfg, "PROCESS_NAME", ""))) if args.manual_start else cfg.PROCESS_NAME

    # Журнал и коннекты
    pg = PGClient(cfg.PG_DSN)
    journal = ProcessJournal(pg, cfg.JOURNAL_TABLE, process_name)
    journal.ensure()

    phx = PhoenixClient(cfg.PQS_URL, fetchmany_size=cfg.PHX_FETCHMANY_SIZE, ts_units=cfg.PHX_TS_UNITS)
    ch  = CHClient(
        hosts=cfg.ch_hosts_list(),
        port=cfg.CH_PORT,
        database=cfg.CH_DB,
        user=cfg.CH_USER,
        password=cfg.CH_PASSWORD,
        compression=True,
    )

    # Таблицы ClickHouse
    ch_table_raw_all   = cfg.CH_RAW_TABLE
    ch_table_clean     = cfg.CH_CLEAN_TABLE
    ch_table_clean_all = cfg.CH_CLEAN_ALL_TABLE
    ch_table_buf       = cfg.CH_DEDUP_BUF_TABLE
    ch_batch = int(cfg.CH_INSERT_BATCH)

    # Времена
    since_dt = parse_iso_utc(args.since) if args.since else journal.get_watermark() or None
    if since_dt is None:
        raise SystemExit("Не задан --since и нет watermark в журнале")
    until_dt = parse_iso_utc(args.until)
    step_min = args.step_min if args.step_min is not None else int(getattr(cfg, "STEP_MIN", 60))
    qshift   = timedelta(minutes=int(getattr(cfg, "PHX_QUERY_SHIFT_MINUTES", 0)))

    # Лаг и захлёст
    phx_lag_min   = int(getattr(cfg, "PHX_QUERY_LAG_MINUTES", 0) or 0)
    phx_overlap_min = int(getattr(cfg, "PHX_QUERY_OVERLAP_MINUTES", 0) or 0)
    overlap_only_first_slice = bool(int(getattr(cfg, "PHX_OVERLAP_ONLY_FIRST_SLICE", 1)))

    lag_delta       = timedelta(minutes=phx_lag_min) if phx_lag_min > 0 else timedelta(0)
    overlap_delta_c = timedelta(minutes=phx_overlap_min) if phx_overlap_min > 0 else timedelta(0)

    # Автокаденс публикаций
    publish_every_min    = int(getattr(cfg, "PUBLISH_EVERY_MINUTES", 60) or 0)   # 0 → выкл
    publish_every_slices = int(getattr(cfg, "PUBLISH_EVERY_SLICES", 0) or 0)     # 0 → выкл
    always_publish_at_end = bool(int(getattr(cfg, "ALWAYS_PUBLISH_AT_END", 1)))

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

    total_read = 0
    total_written_ch = 0
    overall_since: Optional[datetime] = None
    overall_until: Optional[datetime] = None

    host = socket.gethostname()
    pid = os.getpid()

    seen_keys = set()                # локальный дедуп на запуск
    pending_parts: Set[int] = set()  # партиции, требующие публикации
    slices_since_last_pub = 0
    last_pub_dt = journal.last_ok_end(process_name + ":dedup")  # из PG (последний успешный дедуп)

    # Настройки CH для дедуп-агрегации
    ch_dedup_settings = {
        "max_threads": 0,
        "max_memory_usage": 8 * 1024 * 1024 * 1024,                   # 8G
        "max_bytes_before_external_group_by": 2 * 1024 * 1024 * 1024,  # 2G
        "distributed_aggregation_memory_efficient": 1,
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

    def _publish_parts(parts: Set[int]) -> None:
        """Публикация указанных партиций: BUF DROP → BUF INSERT → CLEAN REPLACE → BUF DROP."""
        nonlocal last_pub_dt, slices_since_last_pub
        if not parts:
            return
        # reconnect перед тяжёлыми операциями
        try:
            ch.reconnect()
            log.debug("CH reconnect before publish: OK")
        except Exception as e:
            log.warning("Не удалось переподключиться к CH перед публикацией: %s", e)

        # Под-журнал публикации
        pub_since, pub_until = _parts_to_interval(parts)
        journal_pub = ProcessJournal(pg, cfg.JOURNAL_TABLE, process_name + ":dedup")
        try:
            journal_pub.mark_planned(pub_since, pub_until)
            journal_pub.mark_running(pub_since, pub_until, host=host, pid=pid)
        except Exception as e:
            log.warning("Не удалось создать запись под-процесса для публикации: %s", e)
            journal_pub = None

        non_key_cols = [c for c in CH_COLUMNS if c not in ("c", "t", "opd")]
        agg_exprs = ",\n          ".join([f"argMax({c}, ingested_at) AS {c}" for c in non_key_cols])
        select_cols = "c, t, opd,\n          " + agg_exprs

        for part in sorted(parts):
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
                      {select_cols}
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
            finally:
                pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))

        if journal_pub is not None:
            try:
                journal_pub.mark_done(pub_since, pub_until, rows_read=0, rows_written=0)
            except Exception as e:
                log.debug("Не удалось завершить запись под-процесса дедупа: %s", e)

        # сброс триггеров
        slices_since_last_pub = 0
        last_pub_dt = datetime.now(timezone.utc)

    def _maybe_publish_after_slice() -> None:
        """Проверяем каденс после слайда и при необходимости публикуем накопленные партиции."""
        if args.no_publish:
            return
        if args.force_publish:
            _publish_parts(pending_parts)
            pending_parts.clear()
            return
        # по числу слайсов
        if publish_every_slices > 0 and slices_since_last_pub >= publish_every_slices:
            _publish_parts(pending_parts)
            pending_parts.clear()
            return
        # по времени
        if publish_every_min > 0:
            base_dt = last_pub_dt or journal.last_ok_end(process_name + ":dedup")
            if base_dt is not None:
                now_utc = datetime.now(timezone.utc)
                if (now_utc - base_dt).total_seconds() / 60.0 >= publish_every_min:
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

            is_first_slice = True
            for s, e in iter_slices(since_dt, until_dt, step_min):
                s_q_base = s + qshift
                e_q_base = e + qshift

                use_overlap = overlap_delta_c if (is_first_slice or not overlap_only_first_slice) else timedelta(0)
                s_q = s_q_base - use_overlap if use_overlap > timedelta(0) else s_q_base

                e_q = e_q_base - lag_delta if lag_delta > timedelta(0) else e_q_base
                if e_q <= s_q:
                    e_q = e_q_base  # защитный откат

                # лог окна
                if use_overlap > timedelta(0) or lag_delta > timedelta(0):
                    log.info(
                        "Слайс (бизнес): %s → %s | Phoenix: %s → %s | shift=%+d мин | overlap=%d мин | lag=%d мин",
                        s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
                        int(getattr(cfg, 'PHX_QUERY_SHIFT_MINUTES', 0)),
                        int(use_overlap.total_seconds() // 60),
                        int(lag_delta.total_seconds() // 60),
                    )
                else:
                    log.info(
                        "Слайс (бизнес): %s → %s | запрос к Phoenix: %s → %s | shift=%+d мин",
                        s.isoformat(), e.isoformat(), s_q.isoformat(), e_q.isoformat(),
                        int(getattr(cfg, 'PHX_QUERY_SHIFT_MINUTES', 0))
                    )

                # Уберём возможные «висящие» planned другого окна (после рестартов/крэшей)
                try:
                    journal.clear_conflicting_planned(s, e)
                except Exception:
                    # не критично — продолжаем
                    pass
                try:
                    journal.mark_planned(s, e)
                except UniqueViolation:
                    # гонка/конфликт: подчистим planned и повторим
                    journal.clear_conflicting_planned(s, e)
                    journal.mark_planned(s, e)

                run_id = journal.mark_running(s, e, host=host, pid=pid)

                rows_read = 0
                ch_rows: List[tuple] = []

                overall_since = s_q if overall_since is None else min(overall_since, s_q)
                overall_until = e_q if overall_until is None else max(overall_until, e_q)

                try:
                    s_q_phx = s_q.replace(tzinfo=None) if s_q.tzinfo else s_q
                    e_q_phx = e_q.replace(tzinfo=None) if e_q.tzinfo else e_q

                    for batch in phx.fetch_increment(cfg.HBASE_MAIN_TABLE, cfg.HBASE_MAIN_TS_COLUMN, effective, s_q_phx, e_q_phx):
                        rows_read += len(batch)
                        for r in batch:
                            key = (r.get("c"), _as_int(r.get("t")), _to_dt64_obj(r.get("opd")))
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)
                            ch_rows.append(_row_to_ch_tuple(r))

                        if len(ch_rows) >= ch_batch:
                            total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                            ch_rows.clear()
                            journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})

                    if ch_rows:
                        total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                        ch_rows.clear()

                    # накопим партиции текущего слайда
                    for p in _iter_partitions_by_day(s_q, e_q):
                        pending_parts.add(p)
                    slices_since_last_pub += 1

                    is_first_slice = False
                    journal.mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
                    total_read += rows_read

                    # проверка каденса
                    _maybe_publish_after_slice()

                except Exception as ex:
                    journal.heartbeat(run_id, progress={"error": str(ex)})
                    journal.mark_error(s, e, message=str(ex))
                    raise

            # финальная публикация (по умолчанию включена)
            if (always_publish_at_end or args.force_publish) and pending_parts and not args.no_publish:
                log.info("Финальная публикация: %d партиций.", len(pending_parts))
                _publish_parts(pending_parts)
                pending_parts.clear()

        if int(getattr(cfg, "JOURNAL_RETENTION_DAYS", 0) or 0) > 0:
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