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
      * <schema>.inc_processing      — журнал запусков (planned|running|ok|error|skipped)
      * <schema>.inc_process_state   — watermark (до какого бизнес-времени обработано)

    Основные принципы:
      - В один момент времени может быть только ОДНА «активная» запись по процессу
        (status in ('planned','running') and ts_end is null) — это гарантирует частичный UNIQUE индекс.
      - Переход planned→running выполняется атомарно (UPDATE по найденному planned).
      - Все метаданные и показатели складываются в колонку JSONB `details`.
      - Watermark — правая граница обработанного бизнес-интервала (UTC).
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
        """Возвращает «хвост» имени объекта БД (без схемы), чтобы формировать имена индексов."""
        tail = ident.split(".")[-1]
        return tail.strip('"')

    @staticmethod
    def _derive_state_table_name(journal_table: str) -> str:
        """Строит имя state-таблицы по схеме журнала: <schema>.inc_process_state."""
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

        # Перестраховка: аккуратно переопределяем CHECK по статусам.
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

        # Индексы: быстрые выборки и частичный UNIQUE на одну активную запись
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

        # State-таблица
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
        """
        Любой datetime → UTC-aware. Если пришёл naive, считаем его UTC и помечаем tzinfo=UTC.
        """
        if isinstance(v, datetime):
            return (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
        raise TypeError("expected datetime")

    def _slice_iso_texts(self, slice_from: datetime, slice_to: datetime) -> Tuple[str, str]:
        """
        Приводит границы среза к ISO-строкам в UTC.
        Нужен для корректной работы JSONB/текстовых сравнений в SQL.
        """
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        return sf, st

    def resolve_active_conflicts_for_slice(self, slice_from: datetime, slice_to: datetime) -> Tuple[int, int]:
        """
        Мягко разрешает конфликты «активных» записей (planned|running) для *других* срезов:
          - planned → skipped (reason=conflict_new_slice)
          - running → error  (reason=conflict_new_slice)

        Возвращает кортеж (planned_skipped, running_errored).
        Важно: новые значения границ пишем как ::text, иначе PG не выведет тип (IndeterminateDatatype).
        """
        sf, st = self._slice_iso_texts(slice_from, slice_to)

        # planned (другого окна) → skipped
        self.pg.execute(f"""
        UPDATE {self.table} t
           SET status='skipped',
               ts_end=now(),
               details = COALESCE(t.details,'{{}}'::jsonb)
                         || jsonb_build_object('sanitized', true,
                                               'reason','conflict_new_slice',
                                               'new_slice_from', %s::text,
                                               'new_slice_to',   %s::text,
                                               'sanitized_at', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
         WHERE t.process_name=%s
           AND t.status='planned'
           AND t.ts_end IS NULL
           AND ( (t.details->>'slice_from') IS DISTINCT FROM %s
              OR (t.details->>'slice_to')   IS DISTINCT FROM %s )
        RETURNING t.id
        """, (sf, st, self.process_name, sf, st))
        planned_skipped = len(self.pg.fetchall() or [])

        # running (другого окна) → error
        self.pg.execute(f"""
        UPDATE {self.table} t
           SET status='error',
               ts_end=now(),
               details = COALESCE(t.details,'{{}}'::jsonb)
                         || jsonb_build_object('sanitized', true,
                                               'reason','conflict_new_slice',
                                               'new_slice_from', %s::text,
                                               'new_slice_to',   %s::text,
                                               'sanitized_at', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
         WHERE t.process_name=%s
           AND t.status='running'
           AND t.ts_end IS NULL
           AND ( (t.details->>'slice_from') IS DISTINCT FROM %s
              OR (t.details->>'slice_to')   IS DISTINCT FROM %s )
        RETURNING t.id
        """, (sf, st, self.process_name, sf, st))
        running_errored = len(self.pg.fetchall() or [])

        if planned_skipped or running_errored:
            log.warning(
                "resolve_active_conflicts_for_slice(): planned→skipped=%d, running→error=%d",
                planned_skipped, running_errored
            )
        return planned_skipped, running_errored

    # -------------------- эксклюзив --------------------

    def try_acquire_exclusive_lock(self) -> bool:
        """Пробуем взять advisory-lock на имя процесса (true/false)."""
        self.pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (self.process_name,))
        got = bool(self.pg.fetchone()[0])
        self._lock_acquired = got
        return got

    def release_exclusive_lock(self) -> None:
        """Отпускаем захваченный advisory-lock."""
        if self._lock_acquired:
            self.pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (self.process_name,))
            self._lock_acquired = False

    @contextmanager
    def exclusive_lock(self):
        """
        Контекстный менеджер для «глобального» взаимного исключения по процессу.
        """
        got = self.try_acquire_exclusive_lock()
        try:
            yield got
        finally:
            if got:
                self.release_exclusive_lock()

    # -------------------- действия --------------------

    def mark_planned(self, slice_from: datetime, slice_to: datetime) -> int:
        """
        Регистрирует новый запуск со статусом 'planned' для заданного бизнес-среза.

        Защита от гонок и дублей:
          1) Сначала проверяем, нет ли уже активной записи (planned|running) по процессу.
             Если есть — возвращаем её id (и не делаем INSERT).
          2) Если активной записи нет — пробуем вставить planned.
             В редкой гонке ловим UniqueViolation и возвращаем уже существующую активную запись.
        """
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        payload = {"slice_from": sf, "slice_to": st, "planned": True}

        # 1) Предварительная проверка на активную запись (быстро и безопасно)
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

        # 2) Пытаемся вставить planned
        try:
            self.pg.execute(
                f"INSERT INTO {self.table} (process_name, status, details) VALUES (%s, 'planned', %s::jsonb) RETURNING id",
                (self.process_name, Json(payload)),
            )
            rid = self.pg.fetchone()[0]
            log.info("Запланирован запуск %s: id=%s [%s → %s]", self.process_name, rid, sf, st)
            return rid
        except UniqueViolation:
            # Гонка: параллельно уже появился active planned|running — возвращаем его.
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
        """
        Атомарный перевод planned→running для указанного среза.
        Идемпотентность/устойчивость:
          1) если есть planned этого же окна — переводим в running;
          2) если уже есть running этого же окна — переиспользуем (обновляем heartbeat/host/pid);
          3) если висит активный planned/running другого окна — мягко снимаем конфликт и создаём новую running.
        """
        sf, st = self._slice_iso_texts(slice_from, slice_to)
        upd = {
            "slice_from": sf,
            "slice_to": st,
            "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        # 1) Пытаемся planned→running (м.б. UniqueViolation из-за другого active)
        try:
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
        except UniqueViolation:
            # Активный running уже есть — чистим конфликты и повторяем попытку
            self.resolve_active_conflicts_for_slice(slice_from, slice_to)
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

        # 2) Переиспользуем существующую running этого же окна (после рестарта)
        self.pg.execute(
            f"""SELECT id FROM {self.table}
                 WHERE process_name=%s AND status='running' AND ts_end IS NULL
                   AND (details->>'slice_from')=%s AND (details->>'slice_to')=%s
                 ORDER BY ts_start DESC LIMIT 1""",
            (self.process_name, sf, st)
        )
        row = self.pg.fetchone()
        if row:
            rid = row[0]
            # обновим host/pid и heartbeat
            hb = {"heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
            self.pg.execute(
                f"UPDATE {self.table} SET host=COALESCE(%s,host), pid=COALESCE(%s,pid), details=COALESCE(details,'{{}}'::jsonb)||%s::jsonb WHERE id=%s",
                (host, pid, Json(hb), rid)
            )
            log.info("running (reuse): id=%s [%s → %s]", rid, sf, st)
            self._current_run_id = rid
            return rid

        # 3) Конфликты других окон — снимаем и создаём новую running
        self.resolve_active_conflicts_for_slice(slice_from, slice_to)
        meta = {
            "slice_from": sf, "slice_to": st,
            "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        try:
            self.pg.execute(
                f"INSERT INTO {self.table} (process_name, status, details, host, pid) VALUES (%s,'running',%s::jsonb,%s,%s) RETURNING id",
                (self.process_name, Json(meta), host, pid)
            )
        except UniqueViolation:
            # На случай гонки между очисткой и вставкой — пробуем ещё раз переиспользовать running этого окна
            self.pg.execute(
                f"""SELECT id FROM {self.table}
                     WHERE process_name=%s AND status='running' AND ts_end IS NULL
                       AND (details->>'slice_from')=%s AND (details->>'slice_to')=%s
                     ORDER BY ts_start DESC LIMIT 1""",
                (self.process_name, sf, st)
            )
            row = self.pg.fetchone()
            if row:
                rid = row[0]
                if host or pid:
                    self.pg.execute(
                        f"UPDATE {self.table} SET host=COALESCE(%s,host), pid=COALESCE(%s,pid) WHERE id=%s",
                        (host, pid, rid)
                    )
                log.info("running (reuse after UniqueViolation): id=%s [%s → %s]", rid, sf, st)
                self._current_run_id = rid
                return rid
            # если и тут никого — отдаём исключение наверх
            raise

        rid = self.pg.fetchone()[0]
        log.info("running (new): id=%s [%s → %s]", rid, sf, st)
        self._current_run_id = rid
        return rid

    def heartbeat(self, run_id: Optional[int] = None, progress: Optional[Dict[str, Any]] = None) -> None:
        """Обновляет heartbeat текущего (или указанного) running-запуска и опциональный прогресс."""
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
        """
        Завершает текущий срез (status='ok'), пишет фактические метрики и обновляет watermark правой границей среза.
        """
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
        """Фиксирует ошибку для текущего среза (status='error') без изменения watermark."""
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

    # -------------------- watermark и вспомогательные --------------------

    def get_watermark(self) -> Optional[datetime]:
        """Текущий watermark для процесса (UTC) или None, если ещё не выставлялся."""
        self.pg.execute(f"SELECT watermark FROM {self.state_table} WHERE process_name=%s", (self.process_name,))
        row = self.pg.fetchone()
        return row[0] if row else None

    def last_ok_end(self, process_name: Optional[str] = None) -> Optional[datetime]:
        """
        Возвращает ts_end последнего успешного запуска указанного процесса
        (или текущего self.process_name, если не указан).
        Полезно для автокаденса публикаций.
        """
        pname = process_name or self.process_name
        self.pg.execute(
            f"SELECT ts_end FROM {self.table} "
            "WHERE process_name=%s AND status='ok' AND ts_end IS NOT NULL "
            "ORDER BY ts_end DESC LIMIT 1",
            (pname,)
        )
        row = self.pg.fetchone()
        return row[0] if row else None

    def ok_count_since(self, since_ts: datetime, process_name: Optional[str] = None) -> int:
        """
        Количество успешных запусков (status='ok') указанного процесса с момента since_ts.
        Используется для логики PUBLISH_EVERY_SLICES.
        """
        pname = process_name or self.process_name
        self.pg.execute(
            f"SELECT count(*) FROM {self.table} "
            "WHERE process_name=%s AND status='ok' AND ts_start >= %s",
            (pname, since_ts)
        )
        row = self.pg.fetchone()
        try:
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def clear_conflicting_planned(self, slice_from: datetime, slice_to: datetime) -> int:
        """
        Переводит любые активные planned для ЭТОГО процесса со срезами, отличными от (slice_from, slice_to),
        в статус 'skipped'. Нужно для случаев, когда прошлый процесс упал до mark_running().

        ВАЖНО: параметры new_slice_from/new_slice_to явно приводим к text,
        чтобы PostgreSQL не ругался 'could not determine data type of parameter $1'
        внутри jsonb_build_object (иначе тип 'unknown' не выводится).
        """
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        self.pg.execute(f"""
        UPDATE {self.table} t
           SET status='skipped',
               ts_end=now(),
               details = COALESCE(t.details,'{{}}'::jsonb)
                        || jsonb_build_object('sanitized', true,
                                              'reason','superseded_by_new_slice',
                                              'new_slice_from', %s::text,
                                              'new_slice_to', %s::text,
                                              'sanitized_at', to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
         WHERE t.process_name=%s
           AND t.status='planned'
           AND t.ts_end IS NULL
           AND ((t.details->>'slice_from') IS DISTINCT FROM %s
             OR (t.details->>'slice_to')   IS DISTINCT FROM %s)
        RETURNING t.id
        """, (sf, st, self.process_name, sf, st))
        rows = self.pg.fetchall() or []
        n = len(rows)
        if n:
            log.warning("clear_conflicting_planned(): перевели planned→skipped, строк: %d.", n)
        return n

    def prune_old(self, days: Optional[int] = None, batch_size: int = 10000) -> int:
        """
        Мягкая ретенция журнала: удаляет завершённые записи (ok|error|skipped) старше N дней
        батчами по batch_size. Минимизирует длительные блокировки. Возвращает суммарное число удалённых строк.
        """
        # Если days не передан — берём из ENV JOURNAL_RETENTION_DAYS (по умолчанию 30).
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
        Автосанация «висячих» записей:
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