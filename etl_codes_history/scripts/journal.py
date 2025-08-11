# -*- coding: utf-8 -*-
"""
Журнал запусков ETL-процессов + watermark в PostgreSQL.

Таблицы:
1) <schema>.inc_processing
   - статус: planned | running | ok | error | skipped
   - details JSONB (окно среза, прогресс, heartbeat_ts и др.)
2) <schema>.inc_process_state
   - watermark ("обработано до", исключительная верхняя граница) + origin

Что умеет:
- ensure(): создаёт/чинит DDL и индексы (в т.ч. частичный UNIQUE "один активный").
- advisory lock на process_name (pg_try_advisory_lock(hashtext(...))).
- planned→running→ok/error; heartbeat(); sanitize_stale() (TTL/timeout).
- watermark UPSERT с GREATEST.
- purge_older_than_days().
- встроенные миграции + apply_sql_migrations().

Зависимости от PG-клиента:
  pg.execute(sql, params=None) -> None
  pg.fetchone() -> tuple|None
  pg.fetchall() -> list[tuple]
"""

from __future__ import annotations
import logging
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone
from contextlib import contextmanager

log = logging.getLogger("scripts.journal")


class AlreadyRunning(Exception):
    """Выбрасывается, если процесс уже выполняется и эксклюзивный lock не получен."""


class ProcessJournal:
    """
    Обёртка над двумя PG-таблицами:
      * <schema>.inc_processing      — журнал запусков
      * <schema>.inc_process_state   — watermark по process_name
    """

    def __init__(self, pg, table: str, process_name: str, state_table: Optional[str] = None):
        self.pg = pg
        self.table = table
        self.process_name = process_name
        self.state_table = state_table or self._derive_state_table_name(table)
        self._current_run_id: Optional[int] = None
        self._lock_acquired: bool = False

    # -------------------------- Вспомогательные ---------------------------

    @staticmethod
    def _tail_identifier(ident: str) -> str:
        """Последняя часть имени без схемы и внешних кавычек."""
        tail = ident.split(".")[-1]
        if tail.startswith('"') and tail.endswith('"') and len(tail) >= 2:
            tail = tail[1:-1]
        return tail

    def _idx_name(self, base: str) -> str:
        """Имена индексов в едином стиле."""
        t = self._tail_identifier(self.table)
        return f"{t}_{base}_idx"

    @staticmethod
    def _derive_state_table_name(journal_table: str) -> str:
        parts = journal_table.split(".")
        if len(parts) == 2:
            return f"{parts[0]}.inc_process_state"
        return "inc_process_state"

    # --------------------------------- DDL ----------------------------------

    def ensure(self) -> None:
        """Создаёт таблицы журнала и состояния + индексы. Обновляет check-констрейнт статусов."""
        ddl_table = f"""
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
        """
        self.pg.execute(ddl_table)

        # (re)create check constraint для наборов статусов (идемпотентно)
        self.pg.execute(f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE t.relname = '{self._tail_identifier(self.table)}'
                  AND c.conname = 'inc_processing_status_chk'
            ) THEN
                EXECUTE 'ALTER TABLE {self.table} DROP CONSTRAINT inc_processing_status_chk';
            END IF;
        END$$;
        """)
        self.pg.execute(f"""
        ALTER TABLE {self.table}
        ADD CONSTRAINT inc_processing_status_chk
        CHECK (status IN ('planned','running','ok','error','skipped'));
        """)
        log.info("Проверка таблицы журнала: OK (%s)", self.table)

        # Индексы журнала
        idx_started   = self._idx_name("pname_started_desc")
        idx_active_uq = self._idx_name("active_one_uq")
        idx_ended     = self._idx_name("ended_notnull")

        self.pg.execute(f"""
        CREATE INDEX IF NOT EXISTS {idx_started}
            ON {self.table} (process_name, ts_start DESC);
        """)

        # Пересоздать возможный старый "active_only" не-UNIQUE (если где-то остался)
        self.pg.execute(
            f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = "
            f"'{self._tail_identifier(self.table)}_active_only_idx') "
            f"THEN EXECUTE 'DROP INDEX {self._tail_identifier(self.table)}_active_only_idx'; END IF; END$$;"
        )

        # Гарантия единственного активного запуска на процесс
        self.pg.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {idx_active_uq}
            ON {self.table} (process_name)
            WHERE ts_end IS NULL AND status IN ('planned','running');
        """)

        self.pg.execute(f"""
        CREATE INDEX IF NOT EXISTS {idx_ended}
            ON {self.table} (ts_end)
            WHERE ts_end IS NOT NULL;
        """)
        log.info("Проверка индексов журнала: OK (%s, %s, %s)", idx_started, idx_active_uq, idx_ended)

        # Таблица состояния (watermark)
        self.ensure_state()

    def ensure_state(self) -> None:
        ddl_state = f"""
        CREATE TABLE IF NOT EXISTS {self.state_table} (
            process_name TEXT PRIMARY KEY,
            watermark    TIMESTAMPTZ NULL,
            origin       TEXT        NULL,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        self.pg.execute(ddl_state)

        idx_state_upd = f"{self._tail_identifier(self.state_table)}_updated_at_idx"
        idx_state_wm  = f"{self._tail_identifier(self.state_table)}_watermark_idx"
        self.pg.execute(f"CREATE INDEX IF NOT EXISTS {idx_state_upd} ON {self.state_table} (updated_at DESC);")
        self.pg.execute(f"CREATE INDEX IF NOT EXISTS {idx_state_wm}  ON {self.state_table} (watermark);")
        log.info("Проверка таблицы состояния: OK (%s)", self.state_table)

    # ------------------------------- Время ---------------------------------

    @staticmethod
    def _to_aware_utc(v: Any) -> datetime:
        """int/float(epoch[ms])/str(ISO|epoch)/datetime → aware UTC datetime."""
        if isinstance(v, datetime):
            dt = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        if isinstance(v, (int, float)):
            val = float(v)
            if val > 1e12:
                val = val / 1000.0
            return datetime.fromtimestamp(val, tz=timezone.utc)
        if isinstance(v, str):
            s = v.strip()
            if s.endswith("Z"):
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    pass
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                val = float(s)
                if val > 1e12:
                    val = val / 1000.0
                return datetime.fromtimestamp(val, tz=timezone.utc)
        raise ValueError(f"Не удалось распарсить timestamp: {v!r}")

    # ------------------------ Advisory lock (эксклюзив) ---------------------

    def try_acquire_exclusive_lock(self) -> bool:
        """Неблокирующая попытка взять эксклюзив на процесс (advisory lock)."""
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

    # ------------------------------- ЖУРНАЛ ---------------------------------

    def start(self, meta: Optional[Dict[str, Any]] = None,
              host: Optional[str] = None, pid: Optional[int] = None) -> int:
        """
        Исторический старт 'running'. Проверяет, что нет активного 'running'.
        Возвращает id записи.
        """
        sql_active = f"""
        SELECT id
          FROM {self.table}
         WHERE process_name = %s
           AND status = 'running'
           AND ts_end IS NULL
         LIMIT 1
        """
        self.pg.execute(sql_active, (self.process_name,))
        row = self.pg.fetchone()
        if row:
            raise RuntimeError(f"Уже есть активный запуск процесса '{self.process_name}' (id={row[0]}).")

        sql_ins = f"""
        INSERT INTO {self.table} (process_name, status, details, host, pid)
        VALUES (%s, 'running', %s, %s, %s)
        RETURNING id
        """
        self.pg.execute(sql_ins, (self.process_name, meta, host, pid))
        new_id = self.pg.fetchone()[0]
        log.info("Запуск процесса зарегистрирован: %s id=%s", self.process_name, new_id)
        self._current_run_id = new_id
        return new_id

    def mark_planned(self, slice_from: Any, slice_to: Any,
                     extra: Optional[Dict[str, Any]] = None) -> int:
        """Записать planned для конкретного среза."""
        sf_dt = self._to_aware_utc(slice_from)
        st_dt = self._to_aware_utc(slice_to)
        sf = sf_dt.isoformat()
        st = st_dt.isoformat()

        payload: Dict[str, Any] = {"slice_from": sf, "slice_to": st, "planned": True}
        if extra:
            payload.update(extra)

        sql = f"""
        INSERT INTO {self.table} (process_name, status, details)
        VALUES (%s, 'planned', %s)
        RETURNING id
        """
        self.pg.execute(sql, (self.process_name, payload))
        rid = self.pg.fetchone()[0]
        log.info("Запланирован запуск %s: id=%s [%s → %s]", self.process_name, rid, sf, st)
        return rid

    def mark_running(self, slice_from: Any, slice_to: Any,
                     extra: Optional[Dict[str, Any]] = None,
                     host: Optional[str] = None, pid: Optional[int] = None) -> int:
        """
        planned→running для того же окна (если найден), иначе создание новой 'running'.
        Возвращает id и помечает его как текущий.
        """
        sf_dt = self._to_aware_utc(slice_from)
        st_dt = self._to_aware_utc(slice_to)
        sf = sf_dt.isoformat()
        st = st_dt.isoformat()

        upd_sql = f"""
        WITH cand AS (
            SELECT id
              FROM {self.table}
             WHERE process_name = %s
               AND status = 'planned'
               AND (details->>'slice_from') = %s
               AND (details->>'slice_to')   = %s
             ORDER BY ts_start DESC
             LIMIT 1
        )
        UPDATE {self.table} t
           SET status = 'running',
               ts_start = now(),
               details = COALESCE(t.details, '{{}}'::jsonb) ||
                         COALESCE(%s::jsonb, '{{}}'::jsonb) ||
                         jsonb_build_object(
                           'slice_from', %s::text,
                           'slice_to',   %s::text,
                           'heartbeat_ts', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')
                         )
          FROM cand
         WHERE t.id = cand.id
        RETURNING t.id
        """
        self.pg.execute(upd_sql, (self.process_name, sf, st, extra, sf, st))
        row = self.pg.fetchone()
        if row:
            rid = row[0]
            if host or pid:
                self.pg.execute(
                    f"UPDATE {self.table} SET host=COALESCE(%s, host), pid=COALESCE(%s, pid) WHERE id=%s",
                    (host, pid, rid)
                )
            log.info("planned→running: id=%s [%s → %s]", rid, sf, st)
            self._current_run_id = rid
            return rid

        # Иначе — обычный старт running с указанным окном
        meta = {
            "slice_from": sf,
            "slice_to": st,
            "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        if extra:
            meta.update(extra)
        rid = self.start(meta=meta, host=host, pid=pid)
        log.info("running (new): id=%s [%s → %s]", rid, sf, st)
        return rid

    def heartbeat(self, run_id: Optional[int] = None, progress: Optional[Dict[str, Any]] = None) -> None:
        """Обновить heartbeat текущего running-запуска (и опционально прогресс)."""
        rid = run_id or self._current_run_id
        if not rid:
            return
        sql = f"""
        UPDATE {self.table}
           SET details = COALESCE(details, '{{}}'::jsonb)
                         || jsonb_build_object(
                              'heartbeat_ts', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')
                            )
                         || COALESCE(%s::jsonb, '{{}}'::jsonb)
         WHERE id = %s
           AND process_name = %s
           AND status = 'running'
           AND ts_end IS NULL
        """
        self.pg.execute(sql, (progress, rid, self.process_name))

    def mark_done(self, slice_from: Any, slice_to: Any,
                  rows_read: int, rows_written: int, message: Optional[str] = None) -> None:
        """Закрыть запуск как OK и обновить watermark до конца среза."""
        sf_dt = self._to_aware_utc(slice_from)
        st_dt = self._to_aware_utc(slice_to)
        sf = sf_dt.isoformat(); st = st_dt.isoformat()

        # ЯВНОЕ приведение типов для jsonb_build_object → без ошибок IndeterminateDatatype
        upd_sql = f"""
        UPDATE {self.table} t
           SET status = 'ok',
               ts_end = now(),
               details = COALESCE(t.details, '{{}}'::jsonb)
                         || jsonb_build_object(
                              'rows_read', %s::bigint,
                              'rows_written', %s::bigint,
                              'message', %s::text
                            )
         WHERE t.process_name = %s
           AND t.status = 'running'
           AND t.ts_end IS NULL
           AND (t.details->>'slice_from') = %s
           AND (t.details->>'slice_to')   = %s
        RETURNING t.id
        """
        self.pg.execute(upd_sql, (rows_read, rows_written, ("" if message is None else str(message)),
                                  self.process_name, sf, st))
        row = self.pg.fetchone()
        rid = row[0] if row else None
        log.info("Запуск %s завершён: OK", rid if rid else "-")

        # Обновляем watermark, только если новый больше старого
        wm_sql = f"""
        INSERT INTO {self.state_table} (process_name, watermark, origin, updated_at)
        VALUES (%s, %s, 'pg:finish_ok', now())
        ON CONFLICT (process_name) DO UPDATE
           SET watermark = GREATEST(EXCLUDED.watermark, {self.state_table}.watermark),
               origin    = CASE
                             WHEN EXCLUDED.watermark > {self.state_table}.watermark THEN 'pg:finish_ok'
                             ELSE {self.state_table}.origin
                           END,
               updated_at = now()
        """
        self.pg.execute(wm_sql, (self.process_name, st_dt))
        log.info("Watermark обновлён: %s = %s (pg:finish_ok)", self.process_name, st_dt.isoformat())

    def mark_error(self, slice_from: Any, slice_to: Any, message: str) -> None:
        """Закрыть запуск как ERROR (не меняя watermark)."""
        sf_dt = self._to_aware_utc(slice_from)
        st_dt = self._to_aware_utc(slice_to)
        sf = sf_dt.isoformat(); st = st_dt.isoformat()

        upd_sql = f"""
        UPDATE {self.table} t
           SET status = 'error',
               ts_end = now(),
               details = COALESCE(t.details, '{{}}'::jsonb)
                         || jsonb_build_object('error', %s::text)
         WHERE t.process_name = %s
           AND t.status = 'running'
           AND t.ts_end IS NULL
           AND (t.details->>'slice_from') = %s
           AND (t.details->>'slice_to')   = %s
        """
        self.pg.execute(upd_sql, (str(message), self.process_name, sf, st))
        log.info("Запуск завершён: ERROR: %s", message)

    # ------------------------------- WATERMARK ------------------------------

    def get_watermark(self) -> Optional[datetime]:
        """Вернуть watermark (datetime) либо None."""
        sql = f"SELECT watermark FROM {self.state_table} WHERE process_name = %s"
        self.pg.execute(sql, (self.process_name,))
        row = self.pg.fetchone()
        return row[0] if row else None

    def seed_from_hbase_max_ts(self, mx: Optional[Any]) -> None:
        """Инициализировать watermark значением из внешнего источника (например, HBase)."""
        if mx is None:
            log.warning("Инициализация watermark из HBase пропущена: MAX(TS) не получен (None)")
            return
        dt = self._to_aware_utc(mx)
        sql = f"""
        INSERT INTO {self.state_table} (process_name, watermark, origin, updated_at)
        VALUES (%s, %s, 'hb:max_ts', now())
        ON CONFLICT (process_name) DO UPDATE
          SET watermark = GREATEST(EXCLUDED.watermark, {self.state_table}.watermark),
              origin    = CASE
                            WHEN EXCLUDED.watermark > {self.state_table}.watermark THEN 'hb:max_ts'
                            ELSE {self.state_table}.origin
                          END,
              updated_at = now()
        """
        self.pg.execute(sql, (self.process_name, dt))
        log.info("Watermark инициализирован из HBase: %s = %s", self.process_name, dt.isoformat())

    # ------------------------------- Ретеншн/санация ------------------------

    def purge_older_than_days(self, days: int) -> int:
        """Удалить завершённые записи старше N дней. Возвратить количество удалённых."""
        sql = f"""
        DELETE FROM {self.table}
         WHERE ts_end IS NOT NULL
           AND ts_end < now() - INTERVAL '{int(days)} days'
        RETURNING id
        """
        self.pg.execute(sql)
        rows = self.pg.fetchall() or []
        cnt = len(rows)
        log.info("Очистка журнала: удалено строк = %d (старше %d дней)", cnt, days)
        return cnt

    def sanitize_stale(
        self,
        planned_ttl_minutes: int = 60,
        running_heartbeat_timeout_minutes: int = 45,
        running_hard_ttl_hours: Optional[int] = 12,
    ) -> None:
        """
        Помечает «висячие» planned/running:
        - planned без ts_end, старше planned_ttl_minutes → status='skipped'
        - running без ts_end, если heartbeat слишком старый → status='error'
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
        skipped = len(self.pg.fetchall() or [])

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
        hb_err = len(self.pg.fetchall() or [])

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

        if skipped or hb_err or hard_err:
            log.warning("sanitize_stale(): planned→skipped=%d, running hb→error=%d, running hard→error=%d",
                        skipped, hb_err, hard_err)

    # ------------------------------- Миграции -------------------------------

    def apply_sql_migrations(self) -> List[Tuple[str, int]]:
        """
        Прогоняет встроенные SQL-миграции (идемпотентно).
        Возвращает список (migration_name, 1) для применённых миграций.
        """
        applied: List[Tuple[str, int]] = []
        for name, sql in _EMBEDDED_MIGRATIONS:
            self.pg.execute(sql)
            log.info("Миграция применена: %s", name)
            applied.append((name, 1))
        return applied


# ---- Встроенные миграции (идентичны *.sql в /migrations) ----

_MIGRATION_001 = """
-- 001_bootstrap_journal.sql
BEGIN;
CREATE TABLE IF NOT EXISTS public.inc_processing (
    id            BIGSERIAL PRIMARY KEY,
    process_name  TEXT        NOT NULL,
    ts_start      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ts_end        TIMESTAMPTZ NULL,
    status        TEXT        NOT NULL,
    details       JSONB       NULL,
    host          TEXT        NULL,
    pid           INTEGER     NULL
);
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'inc_processing'
      AND c.conname = 'inc_processing_status_chk'
  ) THEN
    EXECUTE 'ALTER TABLE public.inc_processing DROP CONSTRAINT inc_processing_status_chk';
  END IF;
END$$;
ALTER TABLE public.inc_processing
  ADD CONSTRAINT inc_processing_status_chk
  CHECK (status IN ('planned','running','ok','error','skipped'));
CREATE INDEX IF NOT EXISTS inc_processing_pname_started_desc_idx
  ON public.inc_processing (process_name, ts_start DESC);
-- старый активный индекс мог быть не-UNIQUE — оставим как есть,
-- уникальную защиту создаст миграция 002 (или ensure()).
CREATE INDEX IF NOT EXISTS inc_processing_active_only_idx
  ON public.inc_processing (process_name)
  WHERE ts_end IS NULL AND status IN ('planned','running');
CREATE INDEX IF NOT EXISTS inc_processing_ended_notnull_idx
  ON public.inc_processing (ts_end)
  WHERE ts_end IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.inc_process_state (
    process_name TEXT PRIMARY KEY,
    watermark    TIMESTAMPTZ NULL,
    origin       TEXT        NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS inc_process_state_updated_at_idx
  ON public.inc_process_state (updated_at DESC);
CREATE INDEX IF NOT EXISTS inc_process_state_watermark_idx
  ON public.inc_process_state (watermark);
COMMIT;
"""

_MIGRATION_002 = """
-- 002_one_active_guard.sql
BEGIN;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname  = 'inc_processing_one_active_unique_idx'
  ) THEN
    EXECUTE '
      CREATE UNIQUE INDEX inc_processing_one_active_unique_idx
        ON public.inc_processing (process_name)
        WHERE ts_end IS NULL AND status IN (''planned'',''running'');';
  END IF;
END$$;
COMMIT;
"""

_EMBEDDED_MIGRATIONS: List[Tuple[str, str]] = [
    ("001_bootstrap_journal.sql", _MIGRATION_001),
    ("002_one_active_guard.sql", _MIGRATION_002),
]