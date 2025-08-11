# scripts/smoke_test.py
# -*- coding: utf-8 -*-
"""
Smoke-test для журнала инкрементальной выгрузки.

Что проверяем:
1) ensure() + встроенные миграции применяются без ошибок.
2) advisory lock: второй экземпляр не сможет захватить лок.
3) planned → running → ok: watermark поднимается до конца среза.
4) planned → running → error: watermark не повышается.
5) sanitize_stale(): помечает «висячие» planned (TTL) и running (старый heartbeat/отсутствует).

Запуск:
  python -m scripts.smoke_test \
    --process-name codes_history_smoketest \
    --step-min 5 \
    --keep   # необязательно: оставить записи в БД для визуального осмотра

Опирается на ваш Settings() / .env, подключается к тем же БД.
"""

import logging
import argparse
from datetime import datetime, timedelta, timezone

from .logging_setup import setup_logging
from .config import Settings, parse_iso_utc
from .db.pg_client import PGClient
from .journal import ProcessJournal

log = logging.getLogger("smoke_test")


def _utc_now():
    return datetime.now(timezone.utc)


def assert_true(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def test_ensure_and_migrations(pg: PGClient, journal: ProcessJournal):
    log.info("1) ensure() + apply_sql_migrations()")
    journal.ensure()
    journal.apply_sql_migrations()
    # проверим, что уникальный индекс существует
    idx_name = f"{journal._tail_identifier(journal.table)}_active_one_uq_idx"
    pg.execute(
        "SELECT 1 FROM pg_indexes WHERE schemaname = split_part(%s,'.',1) AND indexname = %s",
        (journal.table, idx_name),
    )
    assert_true(pg.fetchone() is not None, f"Не найден уникальный индекс: {idx_name}")
    log.info("OK: схема и индексы в порядке")


def test_exclusive_lock(cfg: Settings):
    log.info("2) advisory lock (эксклюзив)")
    pg1 = PGClient(cfg.PG_DSN)
    pg2 = PGClient(cfg.PG_DSN)
    j1 = ProcessJournal(pg1, cfg.JOURNAL_TABLE, cfg.PROCESS_NAME + "_smoke_lock")
    j2 = ProcessJournal(pg2, cfg.JOURNAL_TABLE, cfg.PROCESS_NAME + "_smoke_lock")
    j1.ensure()

    got1 = j1.try_acquire_exclusive_lock()
    got2 = j2.try_acquire_exclusive_lock()
    try:
        assert_true(got1 is True, "Первая сессия не смогла взять lock")
        assert_true(got2 is False, "Вторая сессия неожиданно взяла тот же lock")
        log.info("OK: второй инстанс лок не получил")
    finally:
        j1.release_exclusive_lock()
        pg1.close()
        pg2.close()


def test_ok_flow(journal: ProcessJournal):
    log.info("3) planned→running→ok + повышение watermark")
    now = _utc_now()
    s = now - timedelta(hours=2)
    e = now - timedelta(hours=1)

    wm_before = journal.get_watermark()
    journal.mark_planned(s, e)
    journal.mark_running(s, e)

    journal.heartbeat(progress={"phase": "reading", "rows_read": 0})
    journal.mark_done(s, e, rows_read=0, rows_written=0, message="smoke ok")

    wm_after = journal.get_watermark()
    assert_true(wm_after is not None, "Watermark не установлен")
    assert_true(wm_after >= e, f"Watermark не поднялся до {e.isoformat()}, теперь: {wm_after}")
    if wm_before:
        assert_true(wm_after >= wm_before, "Watermark уменьшился — так быть не должно")
    log.info("OK: watermark поднялся до конца среза")


def test_error_flow(journal: ProcessJournal):
    log.info("4) planned→running→error без изменения watermark")
    now = _utc_now()
    s = now - timedelta(hours=1)
    e = now - timedelta(minutes=30)

    wm_before = journal.get_watermark()
    journal.mark_planned(s, e)
    journal.mark_running(s, e)
    journal.mark_error(s, e, message="smoke error")
    wm_after = journal.get_watermark()
    # допускаем None==None; главное — не вырос
    assert_true(wm_before == wm_after, "Watermark не должен меняться при ERROR")
    log.info("OK: watermark не изменился на ошибочном срезе")


def test_sanitize_stale(pg: PGClient, journal: ProcessJournal):
    log.info("5) sanitize_stale(): planned TTL и running heartbeat timeout")
    now = _utc_now()

    # Случай planned: устаревший planned → skipped
    s1 = now - timedelta(minutes=30)
    e1 = now - timedelta(minutes=25)
    rid_planned = journal.mark_planned(s1, e1)
    # Старим запись: ts_start << now()
    pg.execute(
        f"UPDATE {journal.table} SET ts_start = now() - interval '10 minutes' WHERE id = %s",
        (rid_planned,),
    )

    # Случай running: удалим heartbeat_ts и устарим ts_start → должен стать error
    s2 = now - timedelta(minutes=24)
    e2 = now - timedelta(minutes=20)
    journal.mark_planned(s2, e2)
    rid_running = journal.mark_running(s2, e2)
    # Удалим heartbeat_ts и застарим ts_start
    pg.execute(
        f"""
        UPDATE {journal.table}
           SET ts_start = now() - interval '10 minutes',
               details  = COALESCE(details, '{{}}'::jsonb) - 'heartbeat_ts'
         WHERE id = %s
        """,
        (rid_running,),
    )

    # Санитарим жёстко: TTL 1 минута, heartbeat timeout 1 минута
    journal.sanitize_stale(planned_ttl_minutes=1, running_heartbeat_timeout_minutes=1, running_hard_ttl_hours=None)

    # Проверим статусы
    pg.execute(
        f"SELECT status FROM {journal.table} WHERE id = %s",
        (rid_planned,),
    )
    st1 = pg.fetchone()[0]
    pg.execute(
        f"SELECT status, details->>'reason' FROM {journal.table} WHERE id = %s",
        (rid_running,),
    )
    r2 = pg.fetchone()
    st2, reason2 = r2[0], r2[1]

    assert_true(st1 == "skipped", f"Ожидали skipped для planned TTL, получили: {st1}")
    assert_true(st2 == "error" and reason2 in ("heartbeat_timeout", "hard_ttl"),
                f"Ожидали error (heartbeat_timeout/hard_ttl) для running, получили: {st2}/{reason2}")

    log.info("OK: sanitize_stale() пометил зависшие planned/running корректно")


def cleanup(pg: PGClient, journal: ProcessJournal):
    log.info("cleanup: удаляем все тестовые записи")
    pg.execute(f"DELETE FROM {journal.table} WHERE process_name = %s", (journal.process_name,))
    pg.execute(f"DELETE FROM {journal.state_table} WHERE process_name = %s", (journal.process_name,))


def main():
    parser = argparse.ArgumentParser(description="Smoke-test журнала инкрементальной выгрузки")
    parser.add_argument("--process-name", default=None, help="Имя процесса для теста (по умолчанию <PROCESS_NAME>_smoketest)")
    parser.add_argument("--keep", action="store_true", help="Не чистить тестовые записи по завершении")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = Settings()

    test_process_name = args.process_name or (cfg.PROCESS_NAME + "_smoketest")
    pg = PGClient(cfg.PG_DSN)
    journal = ProcessJournal(pg, cfg.JOURNAL_TABLE, test_process_name)

    try:
        test_ensure_and_migrations(pg, journal)
        test_exclusive_lock(cfg)
        test_ok_flow(journal)
        test_error_flow(journal)
        test_sanitize_stale(pg, journal)
        log.info("🎉 SMOKE-TEST: ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО.")
    finally:
        if args.keep:
            log.warning("Записи SMOKE-TEST сохранены (флаг --keep).")
        else:
            try:
                cleanup(pg, journal)
                log.info("cleanup: OK")
            except Exception as e:
                log.exception("cleanup: ошибка: %s", e)
        pg.close()


if __name__ == "__main__":
    main()