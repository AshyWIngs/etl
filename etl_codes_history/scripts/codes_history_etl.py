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

# Ошибки драйвера ClickHouse (используем для точечного ретрая при "Unexpected packet" / "EndOfStream")
from clickhouse_driver import errors as ch_errors
# Высокоточный монотонный таймер — для измерения длительности этапов дедупа
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
    parser.add_argument("--manual-start", action="store_true", help="Если указан, журнал ведётся под именем 'manual_<PROCESS_NAME>' (ручной запуск).")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = Settings()

    # Фактическое имя процесса в журнале: для ручных запусков добавляем префикс manual_
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
        compression=True,  # Native+compression → нужны lz4, zstd и clickhouse-cityhash для clickhouse-driver
    )
    # Таблицы ClickHouse: сырой слой (RAW), чистый слой (CLEAN), их Distributed и буфер дедупликации
    ch_table_raw_all   = cfg.CH_RAW_TABLE         # например: stg.daily_codes_history_raw_all
    ch_table_clean     = cfg.CH_CLEAN_TABLE       # например: stg.daily_codes_history
    ch_table_clean_all = cfg.CH_CLEAN_ALL_TABLE   # например: stg.daily_codes_history_all
    ch_table_buf       = cfg.CH_DEDUP_BUF_TABLE   # например: stg.daily_codes_history_dedup_buf
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
    # Границы всего охваченного окна загрузки (UTC, с учётом сдвига qshift) — понадобятся для определения партиций
    overall_since = None
    overall_until = None
    host = socket.gethostname()
    pid = os.getpid()

    # Множество ключей, встреченных в ЭТОМ запуске (дедуп ETL по (c,t,opd))
    seen_keys = set()

    try:
        with journal.exclusive_lock() as got:
            if not got:
                log.warning("Другой инстанс '%s' уже выполняется — выходим.", process_name)
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

                # Обновляем суммарные границы периода (берём уже сдвинутые времена запроса к Phoenix)
                overall_since = s_q if overall_since is None else min(overall_since, s_q)
                overall_until = e_q if overall_until is None else max(overall_until, e_q)

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
                            total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                            ch_rows.clear()
                            journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written_ch})

                    # хвост
                    if ch_rows:
                        total_written_ch += ch.insert_rows(ch_table_raw_all, ch_rows, CH_COLUMNS)
                        ch_rows.clear()

                    journal.mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
                    total_read += rows_read

                except Exception as ex:
                    journal.heartbeat(run_id, progress={"error": str(ex)})
                    journal.mark_error(s, e, message=str(ex))
                    raise

        # ----- ДЕДУП и ПУБЛИКАЦИЯ партиций (после завершения загрузки) -----
        if overall_since is not None and overall_until is not None:
            # Перед тяжёлыми INSERT SELECT/ALTER после серии INSERT'ов в RAW
            # принудительно переподключаемся к CH, чтобы сбросить возможное "подвешенное" состояние сокета.
            try:
                ch.reconnect()
                log.debug("CH reconnect before dedup: OK")
            except Exception as e:
                log.warning("Не удалось переподключиться к CH перед дедупом: %s", e)

            # ------------------------------------------------------------------
            # Под-процесс в журнале для этапа дедупа/публикации:
            # ведём отдельную запись '<основной процесс>:dedup' и будем слать туда heartbeat
            # с подробными метриками времени по этапам (DROP/INSERT/REPLACE/CLEANUP) для каждой партиции.
            # Это позволяет онлайн отслеживать прогресс и «узкие места» без отдельной схемы метрик.
            # ------------------------------------------------------------------
            journal_pub = ProcessJournal(pg, cfg.JOURNAL_TABLE, process_name + ":dedup")
            try:
                # Планируем и переводим под-процесс дедупа в running на весь охваченный интервал
                journal_pub.mark_planned(overall_since, overall_until)
                journal_pub.mark_running(overall_since, overall_until, host=host, pid=pid)
            except Exception as e:
                # Дедуп продолжится даже если под-журнал не удалось создать (не критично для ETL)
                log.warning("Не удалось создать запись под-процесса для дедупа: %s", e)

            # ------------------------------------------------------------------
            # Локальная обёртка для ch.execute(..) с:
            #  * 1 повтором при ch_errors.UnexpectedPacketFromServerError (часто проявляется как EndOfStream)
            #  * измерением длительности выполнения (perf_counter)
            #  * отправкой heartbeat с метаданными события в под-журнал дедупа
            # Возвращает длительность операции в миллисекундах.
            # ------------------------------------------------------------------
            def _ch_exec(sql: str, *, settings: dict | None = None, label: str = "",
                         stage: str | None = None, part: int | None = None) -> int:
                t0 = perf_counter()
                try:
                    ch.execute(sql, settings=settings)
                except ch_errors.UnexpectedPacketFromServerError as ex:
                    # Частый сетевой сбой драйвера при длинных сессиях (особенно после bulk INSERT'ов):
                    # пробуем мягко переподключиться и повторить один раз.
                    log.warning("CH '%s' упал с UnexpectedPacketFromServerError: %s — reconnect() и повтор",
                                label or "exec", ex)
                    try:
                        ch.reconnect()
                    except Exception as re:
                        log.warning("reconnect() перед повтором не удался: %s", re)
                    # второй (и последний) шанс — если снова ошибка, даём ей проброситься наверх
                    ch.execute(sql, settings=settings)
                dur_ms = int((perf_counter() - t0) * 1000)
                # Лог + heartbeat в под-журнал с меткой этапа/партиции
                ev = {"stage": stage or (label or "exec"), "part": part, "ms": dur_ms}
                log.info("Дедуп: %s (part=%s) занял %d мс", ev["stage"], str(ev["part"]), dur_ms)
                try:
                    journal_pub.heartbeat(progress={"dedup_event": ev})
                except Exception as he:
                    # Не прерываем ETL, если журнал временно недоступен
                    log.debug("journal heartbeat (dedup_event) не записан: %s", he)
                return dur_ms

            # 1) Определяем затронутые партиции RAW по диапазону фактического запроса (с учётом qshift)
            #    Берём дни между overall_since (включительно) и overall_until (исключая правую границу).
            #    Это эквивалентно партиционированию PARTITION BY toYYYYMMDD(opd).
            def _iter_partitions_by_day(start_dt: datetime, end_dt: datetime):
                start_day = start_dt.date()
                # правая граница открытая → включаем последний день как (end_dt - 1 мс)
                end_day_inclusive = (end_dt - timedelta(milliseconds=1)).date()
                d = start_day
                while d <= end_day_inclusive:
                    yield int(d.strftime("%Y%m%d"))
                    d += timedelta(days=1)

            parts = list(_iter_partitions_by_day(overall_since, overall_until))
            log.info("Затронутые партиции RAW (toYYYYMMDD, вычислено локально): %s", ", ".join(map(str, parts)) if parts else "—")

            # 2) Для каждой партиции — собрать дедуп в буфер и атомарно заменить партицию в CLEAN.
            # Используем argMax(..., ingested_at), где ingested_at — материализованная колонка в RAW (now64(3)).
            non_key_cols = [c for c in CH_COLUMNS if c not in ("c", "t", "opd")]
            agg_exprs = ",\n          ".join([f"argMax({c}, ingested_at) AS {c}" for c in non_key_cols])
            select_cols = "c, t, opd,\n          " + agg_exprs

            # Щадящие настройки для больших группировок (можно регулировать из .env в будущем)
            ch_dedup_settings = {
                "max_threads": 0,  # авто по числу ядер
                "max_memory_usage": 8 * 1024 * 1024 * 1024,                   # 8G
                "max_bytes_before_external_group_by": 2 * 1024 * 1024 * 1024,  # 2G — спилл на диск
                "distributed_aggregation_memory_efficient": 1,
            }

            for part in parts:
                # Кооперативный pg_advisory_lock на ключ «dedup:part», чтобы не конфликтовать с другими инстансами
                lock_key = f"codes_history:dedup:{part}"
                pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key,))
                got = bool(pg.fetchone()[0])
                if not got:
                    log.warning("Партиция %s: лок занят другим инстансом — пропускаю.", part)
                    continue
                try:
                    log.info("Партиция %s: начинаю дедуп/публикацию…", part)
                    # Общий таймер по партиции: позволит увидеть total-время на полный цикл (DROP→INSERT→REPLACE→CLEANUP)
                    part_t0 = perf_counter()
                    # Чистим буферную партицию, чтобы REPLACE взял ровно свежую выборку (операция идемпотентна)
                    _ch_exec(f"ALTER TABLE {ch_table_buf} DROP PARTITION {part}",
                             label=f"drop buf partition {part}", stage="drop_buf", part=part)

                    # Собираем дедуплицированную партицию в BUF
                    insert_sql = f"""
                        INSERT INTO {ch_table_buf} ({CH_COLUMNS_STR})
                        SELECT
                          {select_cols}
                        FROM {ch_table_raw_all}
                        WHERE toYYYYMMDD(opd) = {part}
                        GROUP BY c, t, opd
                    """
                    _ch_exec(insert_sql, settings=ch_dedup_settings,
                             label=f"insert dedup part {part}", stage="insert_dedup", part=part)

                    # Атомарно заменяем партицию в CLEAN данными из BUF (без дублей)
                    _ch_exec(f"ALTER TABLE {ch_table_clean} REPLACE PARTITION {part} FROM {ch_table_buf}",
                             label=f"replace clean part {part}", stage="replace_clean", part=part)

                    # Удаляем временную партицию из BUF, чтобы не занимала место
                    _ch_exec(f"ALTER TABLE {ch_table_buf} DROP PARTITION {part}",
                             label=f"cleanup buf partition {part}", stage="drop_buf_cleanup", part=part)

                    # Финальный total по партиции: фиксируем совокупное время всех этапов
                    part_ms = int((perf_counter() - part_t0) * 1000)
                    log.info("Дедуп: part=%s total %d мс", part, part_ms)
                    try:
                        journal_pub.heartbeat(progress={"dedup_part_total_ms": {str(part): part_ms}})
                    except Exception as he:
                        log.debug("journal heartbeat (dedup_part_total_ms) не записан: %s", he)

                    log.info("Партиция %s: опубликована в %s.", part, ch_table_clean)
                finally:
                    pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))

            # Закрываем под-процесс дедупа: записываем завершение интервала публикации
            try:
                journal_pub.mark_done(overall_since, overall_until, rows_read=0, rows_written=0)
            except Exception as e:
                # Не критично для самого ETL, если фиксация завершения под-журнала не удалась
                log.debug("Не удалось завершить запись под-процесса дедупа: %s", e)

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
    