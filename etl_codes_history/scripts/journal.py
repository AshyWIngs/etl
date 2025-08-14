# file: scripts/journal.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import logging
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone
from contextlib import contextmanager
from psycopg.types.json import Json
from psycopg.errors import UniqueViolation

log = logging.getLogger("scripts.journal")

class ProcessJournal:
    """
    Журнал инкрементальных запусков + watermark в PostgreSQL.
    Минимально, но достаточно для эксплуатации:
      * <schema>.inc_processing      — журнал запусков (planned|running|ok|error)
      * <schema>.inc_process_state   — watermark (до какого бизнес-времени обработано)
    """

    def __init__(self, pg, table: str, process_name: str, state_table: Optional[str] = None):
        self.pg = pg
        self.table = table
        self.process_name = process_name
        self.state_table = state_table or self._derive_state_table_name(table)
        self._current_run_id: Optional[int] = None
        self._lock_acquired: bool = False

    # -------------------- базовое --------------------

    @staticmethod
    def _tail_identifier(ident: str) -> str:
        tail = ident.split(".")[-1]
        return tail.strip('"')

    @staticmethod
    def _derive_state_table_name(journal_table: str) -> str:
        parts = journal_table.split(".")
        return f"{parts[0]}.inc_process_state" if len(parts) == 2 else "inc_process_state"

    def ensure(self) -> None:
        """
        Создаёт таблицы и индексы, если их нет (идемпотентно).
        """
        self.pg.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            id            BIGSERIAL PRIMARY KEY,
            process_name  TEXT        NOT NULL,
            ts_start      TIMESTAMPTZ NOT NULL DEFAULT now(),
            ts_end        TIMESTAMPTZ NULL,
            status        TEXT        NOT NULL,
            details       JSONB       NULL,
            host          TEXT        NULL,
            pid           INTEGER     NULL
        );
        """)
        # простой check на статусы
        self.pg.execute(f"""
        DO $$
        DECLARE
          v_rel regclass := to_regclass('{self.table.replace("'", "''")}');
          v_constraint text := 'inc_processing_status_chk';
        BEGIN
          IF v_rel IS NOT NULL AND EXISTS (
            SELECT 1
              FROM pg_constraint c
             WHERE c.conrelid = v_rel
               AND c.conname  = v_constraint
          ) THEN
            EXECUTE format('ALTER TABLE %%s DROP CONSTRAINT %%I', v_rel, v_constraint);
          END IF;
        END$$;
        """)
        self.pg.execute(f"""
        ALTER TABLE {self.table}
          ADD CONSTRAINT inc_processing_status_chk
          CHECK (status IN ('planned','running','ok','error','skipped'));
        """)
        # индексы
        self.pg.execute(f"""
        CREATE INDEX IF NOT EXISTS {self._tail_identifier(self.table)}_pname_started_desc_idx
          ON {self.table} (process_name, ts_start DESC);
        """)
        self.pg.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {self._tail_identifier(self.table)}_active_one_uq_idx
          ON {self.table} (process_name)
          WHERE ts_end IS NULL AND status IN ('planned','running');
        """)
        self.pg.execute(f"""
        CREATE INDEX IF NOT EXISTS {self._tail_identifier(self.table)}_ended_notnull_idx
          ON {self.table} (ts_end) WHERE ts_end IS NOT NULL;
        """)
        # state
        self.pg.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.state_table} (
            process_name TEXT PRIMARY KEY,
            watermark    TIMESTAMPTZ NULL,
            origin       TEXT        NULL,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)
        self.pg.execute(f"CREATE INDEX IF NOT EXISTS {self._tail_identifier(self.state_table)}_updated_at_idx ON {self.state_table}(updated_at DESC);")
        self.pg.execute(f"CREATE INDEX IF NOT EXISTS {self._tail_identifier(self.state_table)}_watermark_idx  ON {self.state_table}(watermark);")
        log.info("Проверка таблицы журнала: OK (%s)", self.table)
        log.info("Проверка индексов журнала: OK (%s, %s, %s)",
                 f"{self._tail_identifier(self.table)}_pname_started_desc_idx",
                 f"{self._tail_identifier(self.table)}_active_one_uq_idx",
                 f"{self._tail_identifier(self.table)}_ended_notnull_idx")
        log.info("Проверка таблицы состояния: OK (%s)", self.state_table)

    # -------------------- утилиты времени --------------------

    @staticmethod
    def _to_aware_utc(v: Any) -> datetime:
        if isinstance(v, datetime):
            return (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
        raise TypeError("expected datetime")

    # -------------------- эксклюзив --------------------

    def try_acquire_exclusive_lock(self) -> bool:
        self.pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (self.process_name,))
        got = bool(self.pg.fetchone()[0])
        self._lock_acquired = got
        return got

    def release_exclusive_lock(self) -> None:
        if self._lock_acquired:
            self.pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (self.process_name,))
            self._lock_acquired = False

    @contextmanager
    def exclusive_lock(self):
        got = self.try_acquire_exclusive_lock()
        try:
            yield got
        finally:
            if got:
                self.release_exclusive_lock()

    # -------------------- действия --------------------

    def mark_planned(self, slice_from: datetime, slice_to: datetime) -> int:
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        payload = {"slice_from": sf, "slice_to": st, "planned": True}
        # Если уже есть активный planned/running — не дублируем planned, вернём его id.        
        self.pg.execute(
            f"SELECT id FROM {self.table} "
            "WHERE process_name=%s AND ts_end IS NULL AND status IN ('planned','running') "
            "ORDER BY ts_start DESC LIMIT 1",
            (self.process_name,)
        )
        row = self.pg.fetchone()
        if row:
            rid = row[0]
            log.warning("Активный запуск уже существует (id=%s) — пропускаю вставку planned.", rid)
            return rid
        try:
            self.pg.execute(
                f"INSERT INTO {self.table} (process_name, status, details) VALUES (%s, 'planned', %s::jsonb) RETURNING id",
                (self.process_name, Json(payload)),
            )
            rid = self.pg.fetchone()[0]
            log.info("Запланирован запуск %s: id=%s [%s → %s]", self.process_name, rid, sf, st)
            return rid
        except UniqueViolation:
            # Гонка: параллельно уже появился active planned/running под уникальным индексом.
            self.pg.execute(
                f"SELECT id FROM {self.table} "
                "WHERE process_name=%s AND ts_end IS NULL AND status IN ('planned','running') "
                "ORDER BY ts_start DESC LIMIT 1",
                (self.process_name,)
            )
            row2 = self.pg.fetchone()
            if not row2:
                raise
            rid = row2[0]
            log.warning("Активный запуск уже создан параллельно (id=%s) — использую его.", rid)
            return rid

    def mark_running(self, slice_from: datetime, slice_to: datetime, host: Optional[str] = None, pid: Optional[int] = None) -> int:
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        upd = {
            "slice_from": sf,
            "slice_to": st,
            "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        self.pg.execute(f"""
        WITH cand AS (
            SELECT id FROM {self.table}
             WHERE process_name=%s AND status='planned'
               AND (details->>'slice_from')=%s AND (details->>'slice_to')=%s
             ORDER BY ts_start DESC LIMIT 1
        )
        UPDATE {self.table} t
           SET status='running',
               ts_start=now(),
               details=COALESCE(t.details, '{{}}'::jsonb) || %s::jsonb
          FROM cand
         WHERE t.id=cand.id
        RETURNING t.id
        """, (self.process_name, sf, st, Json(upd)))
        row = self.pg.fetchone()
        if row:
            rid = row[0]
            if host or pid:
                self.pg.execute(
                    f"UPDATE {self.table} SET host=COALESCE(%s,host), pid=COALESCE(%s,pid) WHERE id=%s",
                    (host, pid, rid)
                )
            log.info("planned→running: id=%s [%s → %s]", rid, sf, st)
            self._current_run_id = rid
            return rid
        # если planned не нашли — создадим running
        meta = {
            "slice_from": sf, "slice_to": st,
            "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        self.pg.execute(
            f"INSERT INTO {self.table} (process_name, status, details, host, pid) VALUES (%s,'running',%s::jsonb,%s,%s) RETURNING id",
            (self.process_name, Json(meta), host, pid)
        )
        rid = self.pg.fetchone()[0]
        log.info("running (new): id=%s [%s → %s]", rid, sf, st)
        self._current_run_id = rid
        return rid

    def heartbeat(self, run_id: Optional[int] = None, progress: Optional[Dict[str, Any]] = None) -> None:
        rid = run_id or self._current_run_id
        if not rid:
            return
        upd = {"heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
        if progress:
            upd.update(progress)
        self.pg.execute(
            f"UPDATE {self.table} SET details = COALESCE(details,'{{}}'::jsonb) || %s::jsonb WHERE id=%s AND status='running' AND ts_end IS NULL",
            (Json(upd), rid)
        )

    def mark_done(self, slice_from: datetime, slice_to: datetime, rows_read: int, rows_written: int) -> None:
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        upd = {"rows_read": int(rows_read), "rows_written": int(rows_written)}
        self.pg.execute(f"""
        UPDATE {self.table} t
           SET status='ok', ts_end=now(), details = COALESCE(t.details,'{{}}'::jsonb) || %s::jsonb
         WHERE t.process_name=%s AND t.status='running' AND t.ts_end IS NULL
           AND (t.details->>'slice_from')=%s AND (t.details->>'slice_to')=%s
        RETURNING t.id
        """, (Json(upd), self.process_name, sf, st))
        row = self.pg.fetchone()
        rid = row[0] if row else None
        log.info("Запуск %s завершён: OK", rid if rid else "-")
        self._current_run_id = None

        # обновим watermark (берём правую границу)
        st_dt = self._to_aware_utc(slice_to)
        self.pg.execute(f"""
        INSERT INTO {self.state_table} (process_name, watermark, origin, updated_at)
        VALUES (%s, %s, 'pg:finish_ok', now())
        ON CONFLICT (process_name) DO UPDATE
           SET watermark = GREATEST(EXCLUDED.watermark, {self.state_table}.watermark),
               origin    = CASE WHEN EXCLUDED.watermark > {self.state_table}.watermark THEN 'pg:finish_ok'
                                ELSE {self.state_table}.origin END,
               updated_at = now()
        """, (self.process_name, st_dt))

    def mark_error(self, slice_from: datetime, slice_to: datetime, message: str) -> None:
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        self.pg.execute(f"""
        UPDATE {self.table}
           SET status='error', ts_end=now(),
               details = COALESCE(details,'{{}}'::jsonb) || %s::jsonb
         WHERE process_name=%s AND status='running' AND ts_end IS NULL
           AND (details->>'slice_from')=%s AND (details->>'slice_to')=%s
        """, (Json({"error": str(message)}), self.process_name, sf, st))
        log.info("Запуск завершён: ERROR: %s", message)
        self._current_run_id = None

    # watermark helpers
    def get_watermark(self) -> Optional[datetime]:
        self.pg.execute(f"SELECT watermark FROM {self.state_table} WHERE process_name=%s", (self.process_name,))
        row = self.pg.fetchone()
        return row[0] if row else None


    def prune_old(self, days: Optional[int] = None, batch_size: int = 10000) -> int:
        """
        Мягкая ретенция: удаляет завершённые записи (ok|error|skipped) старше N дней
        батчами по batch_size. Минимизирует длительные блокировки. Возвращает суммарное
        число удалённых строк.
        """
        # Если days не передан — берём из переменной окружения JOURNAL_RETENTION_DAYS.
        # Пустое/отсутствующее значение → используем дефолт 30.
        if days is None:
            import os
            env_val = os.getenv("JOURNAL_RETENTION_DAYS", "").strip()
            if env_val == "":
                days = 30
            else:
                try:
                    days = int(env_val)
                except ValueError:
                    log.warning("JOURNAL_RETENTION_DAYS некорректно (%r) — использую 30.", env_val)
                    days = 30

        # days ≤ 0 — мягкая ретенция отключена.
        if days <= 0:
            log.info("prune_old: отключено (days=%d).", days)
            return 0

        days = int(days)
        batch_size = int(batch_size)
        total = 0
        while True:
            self.pg.execute(f"""
            WITH del AS (
                SELECT id
                  FROM {self.table}
                 WHERE ts_end IS NOT NULL
                   AND status IN ('ok','error','skipped')
                   AND ts_end < now() - INTERVAL '{days} days'
                 ORDER BY ts_end, id
                 LIMIT {batch_size}
            )
            DELETE FROM {self.table} t
              USING del
             WHERE t.id = del.id
            RETURNING t.id
            """)
            rows = self.pg.fetchall() or []
            n = len(rows)
            total += n
            if n < batch_size:
                break
        if total:
            log.warning("prune_old(%d, batch=%d): удалено %d завершённых записей.", days, batch_size, total)
        return total

    def sanitize_stale(
        self,
        planned_ttl_minutes: int = 60,
        running_heartbeat_timeout_minutes: int = 45,
        running_hard_ttl_hours: Optional[int] = 12,
    ) -> None:
        """
        Чистит «висячие» записи:
        - planned без ts_end, старше planned_ttl_minutes → status='skipped'
        - running без ts_end и с протухшим heartbeat → status='error'
        - (опционально) running старше жёсткого TTL часов → status='error'
        """
        # planned → skipped
        sql_planned = f"""
        UPDATE {self.table} t
           SET status = 'skipped',
               ts_end = now(),
               details = COALESCE(t.details, '{{}}'::jsonb)
                         || jsonb_build_object('sanitized', true,
                                               'reason', 'planned_ttl',
                                               'sanitized_at', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
         WHERE t.process_name = %s
           AND t.status = 'planned'
           AND t.ts_end IS NULL
           AND t.ts_start < now() - INTERVAL '{int(planned_ttl_minutes)} minutes'
        RETURNING t.id
        """
        self.pg.execute(sql_planned, (self.process_name,))
        planned_skipped = len(self.pg.fetchall() or [])

        # running с протухшим heartbeat → error
        sql_running_hb = f"""
        UPDATE {self.table} t
           SET status = 'error',
               ts_end = now(),
               details = COALESCE(t.details, '{{}}'::jsonb)
                         || jsonb_build_object('sanitized', true,
                                               'reason', 'heartbeat_timeout',
                                               'sanitized_at', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
         WHERE t.process_name = %s
           AND t.status = 'running'
           AND t.ts_end IS NULL
           AND COALESCE( (t.details->>'heartbeat_ts')::timestamptz, t.ts_start )
               < now() - INTERVAL '{int(running_heartbeat_timeout_minutes)} minutes'
        RETURNING t.id
        """
        self.pg.execute(sql_running_hb, (self.process_name,))
        running_err = len(self.pg.fetchall() or [])

        hard_err = 0
        if running_hard_ttl_hours is not None:
            sql_running_hard = f"""
            UPDATE {self.table} t
               SET status = 'error',
                   ts_end = now(),
                   details = COALESCE(t.details, '{{}}'::jsonb)
                             || jsonb_build_object('sanitized', true,
                                                   'reason', 'hard_ttl',
                                                   'sanitized_at', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
             WHERE t.process_name = %s
               AND t.status = 'running'
               AND t.ts_end IS NULL
               AND t.ts_start < now() - INTERVAL '{int(running_hard_ttl_hours)} hours'
            RETURNING t.id
            """
            self.pg.execute(sql_running_hard, (self.process_name,))
            hard_err = len(self.pg.fetchall() or [])

        if planned_skipped or running_err or hard_err:
            log.warning(
                "sanitize_stale(): planned→skipped=%d, running hb→error=%d, running hard→error=%d",
                planned_skipped, running_err, hard_err
            )