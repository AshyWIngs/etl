# file: scripts/journal.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import logging
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
from psycopg.types.json import Json
from psycopg.errors import UniqueViolation
import os
import time

# Настройка таймзоны вывода для ЛОГОВ (не влияет на UTC в БД)
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # на всякий случай — если модуль недоступен
    ZoneInfo = None  # type: ignore

_LOG_TZ_CONFIGURED = False

class _TzFormatter(logging.Formatter):
    """
    Форматтер, печатающий %(asctime)s в заданной таймзоне.
    Время берём как UTC и переводим в нужный TZ только для логов.
    """
    def __init__(self, fmt: str | None = None, datefmt: str | None = None, tzinfo: timezone | None = None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._tz = tzinfo or timezone.utc
    
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone(self._tz)
        if datefmt:
            return dt.strftime(datefmt)
        # ISO‑8601 без микросекунд для компактности
        return dt.isoformat(timespec="seconds")

def _resolve_log_timezone(name: str) -> timezone:
    """Подбирает tzinfo по имени зоны; если не найдено — fallback на GMT+5."""
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    # Фолбэк: фиксированный сдвиг +05:00 (Астана)
    return timezone(timedelta(hours=5))

def _configure_logger_timezone_once() -> None:
    """
    Один раз перестраивает форматтеры хэндлеров текущего логгера так,
    чтобы время в логах печаталось в бизнес‑таймзоне.
    Управляется переменной окружения JOURNAL_LOG_TZ (рекомендуется 'Asia/Almaty', GMT+5).
    """
    global _LOG_TZ_CONFIGURED
    if _LOG_TZ_CONFIGURED:
        return

    # Берём переменную JOURNAL_LOG_TZ (если не задана — используем 'Asia/Almaty', GMT+5); влияет только на вывод логов.
    tz_name = (os.getenv("JOURNAL_LOG_TZ") or "Asia/Almaty").strip()
    tzinfo = _resolve_log_timezone(tz_name)

    # Обновляем форматтеры у хэндлеров корневого и наших логгеров
    def _each_handler():
        seen = set()
        for lname in ("", "scripts", "scripts.journal"):
            lg = logging.getLogger(lname)
            for h in getattr(lg, "handlers", []) or []:
                hid = id(h)
                if hid in seen:
                    continue
                seen.add(hid)
                yield h

    for h in _each_handler():
        cur_fmt = None
        cur_datefmt = None
        if getattr(h, "formatter", None):
            # Пытаемся максимально сохранить текущий вид форматирования
            try:
                cur_fmt = h.formatter._style._fmt  # type: ignore[attr-defined]
            except Exception:
                cur_fmt = None
            try:
                cur_datefmt = h.formatter.datefmt  # type: ignore[attr-defined]
            except Exception:
                cur_datefmt = None
        # разумные дефолты, если форматтер не был задан
        if cur_fmt is None:
            cur_fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
        if cur_datefmt is None:
            cur_datefmt = "%Y-%m-%d %H:%M:%S"
        h.setFormatter(_TzFormatter(fmt=cur_fmt, datefmt=cur_datefmt, tzinfo=tzinfo))

    _LOG_TZ_CONFIGURED = True

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
      - Для фатальных «стартовых» сбоев (например, не удалось подключиться к внешней БД)
        предусмотрена запись ошибки БЕЗ привязки к бизнес-срезу — через `mark_startup_error(...)`.
        Такая запись сразу создаётся со status='error' и ts_end=now(), поэтому не конфликтует
        с частичным UNIQUE-индексом для active (planned/running).

    Режим «минимального журнала» и троттлинг heartbeat:
      - Для высоконагруженных сценариев можно уменьшить число записей в БД.
      - Если включён минимальный режим, фиксация событий идёт только на ключевых переходах:
        planned → running → ok/error, плюс авто‑санация; фоновые heartbeat без прогресса
        можно не писать вовсе.
      - Порог частоты heartbeat регулируется параметром JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC
        (по умолчанию 30 секунд). До истечения интервала heartbeat без прогресса не пишется.
      - Режим включается либо через параметр конструктора `minimal=True`, либо через
        переменную окружения `JOURNAL_MINIMAL=1|true|yes|on`.

    Часовой пояс ЛОГОВ:
      - Переменная окружения JOURNAL_LOG_TZ (по умолчанию 'Asia/Almaty', GMT+5) задаёт TZ только для печати логов.
      - Все значения времени, которые пишутся в БД (журнал/состояние/метки sanitized_at), остаются строго в UTC.
    """

    def __init__(self, pg, table: str, process_name: str, state_table: Optional[str] = None,
                 minimal: Optional[bool] = None, heartbeat_min_interval_sec: Optional[int] = None):
        _configure_logger_timezone_once()
        self.pg = pg
        self.table = table
        self.process_name = process_name
        self.state_table = state_table or self._derive_state_table_name(table)
        self._current_run_id: Optional[int] = None
        self._lock_acquired: bool = False
        # Режим «минимального журнала» и троттлинг heartbeat.
        # Можно передать явно через параметры конструктора либо задать через ENV.
        env_min = os.getenv("JOURNAL_MINIMAL", "").strip().lower()
        self.minimal: bool = (minimal if minimal is not None else env_min in ("1", "true", "yes", "on"))
        try:
            default_hb_interval = int(os.getenv("JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", "30"))
        except Exception:
            default_hb_interval = 30
        self.hb_min_interval: int = int(heartbeat_min_interval_sec if heartbeat_min_interval_sec is not None else default_hb_interval)
        # монотонные тики для дешёвого контроля частоты обращений к БД
        self._last_hb_mono: float = 0.0

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
        -- ЖУРНАЛ ЗАПУСКОВ: чистое создание таблицы со встроенным CHECK без ALTER'ов.
        CREATE TABLE IF NOT EXISTS {self.table} (
            id            BIGSERIAL PRIMARY KEY,       -- суррогатный PK
            process_name  TEXT        NOT NULL,        -- имя процесса/подпроцесса
            ts_start      TIMESTAMPTZ NOT NULL DEFAULT now(), -- когда стартанули (UTC)
            ts_end        TIMESTAMPTZ NULL,            -- когда завершили (UTC), NULL для активных
            status        TEXT        NOT NULL CHECK (status IN ('planned','running','ok','error','skipped')),
            details       JSONB       NULL,            -- произвольные метаданные/метрики/ошибка
            host          TEXT        NULL,            -- хост, на котором шли
            pid           INTEGER     NULL             -- PID процесса
        );
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

        # STATE: агрегированное состояние процесса и водяной знак публикации.
        # расширенная схема: храним здоровье, конец последнего успешного окна, hb и последнюю ошибку.
        self.pg.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.state_table} (
            process_name          TEXT PRIMARY KEY,           -- процесс/подпроцесс
            last_status           TEXT,                       -- 'ok' | 'error' | 'running' | 'planned' | NULL
            healthy               BOOLEAN,                    -- TRUE когда последний статус 'ok'
            last_ok_end           TIMESTAMPTZ,                -- правая граница последнего УСПЕШНО обработанного окна (UTC)
            last_started_at       TIMESTAMPTZ,                -- последний старт (UTC)
            last_heartbeat        TIMESTAMPTZ,                -- последнее heartbeat (UTC)
            last_error_at         TIMESTAMPTZ,                -- когда упали
            last_error_component  TEXT,                       -- где упали (phoenix|clickhouse|postgres|runtime|config|...)
            last_error_message    TEXT,                       -- короткое описание ошибки
            progress              JSONB,                      -- произвольные метрики
            extra                 JSONB,                      -- расширяемое поле
            updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)
        self.pg.execute(f"CREATE INDEX IF NOT EXISTS {self._tail_identifier(self.state_table)}_updated_at_idx ON {self.state_table}(updated_at DESC);")
        self.pg.execute(f"CREATE INDEX IF NOT EXISTS {self._tail_identifier(self.state_table)}_last_ok_end_idx ON {self.state_table}(last_ok_end);")

        log.info("Проверка таблицы журнала: OK (%s)", self.table)
        log.debug("Проверка индексов журнала: OK (%s, %s, %s)",
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
        Границы хранятся и сравниваются в UTC.
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
        Границы slice_from/slice_to — всегда в UTC.
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
        Границы slice_from/slice_to — всегда в UTC.
        """
        sf, st = self._slice_iso_texts(slice_from, slice_to)
        # slice_from/slice_to всегда в UTC; heartbeat_ts — для первичной метки запуска окна
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
            try:
                self._state_upsert(
                    status="running",
                    healthy=None,
                    last_started_at=datetime.now(timezone.utc)
                )
            except Exception:
                pass
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
            # Даже в минимальном режиме переход в running сохраняется (важная веха).
            self._current_run_id = rid
            try:
                self._state_upsert(
                    status="running",
                    healthy=None,
                    last_started_at=datetime.now(timezone.utc)
                )
            except Exception:
                pass
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
                try:
                    self._state_upsert(
                        status="running",
                        healthy=None,
                        last_started_at=datetime.now(timezone.utc)
                    )
                except Exception:
                    pass
                return rid
            # если и тут никого — отдаём исключение наверх
            raise

        rid = self.pg.fetchone()[0]
        log.info("running (new): id=%s [%s → %s]", rid, sf, st)
        self._current_run_id = rid
        try:
            self._state_upsert(
                status="running",
                healthy=None,
                last_started_at=datetime.now(timezone.utc)
            )
        except Exception:
            pass
        return rid

    def heartbeat(self, run_id: Optional[int] = None, progress: Optional[Dict[str, Any]] = None) -> None:
        """Обновляет heartbeat текущего (или указанного) running-запуска и опциональный прогресс.

        Оптимизации под нагрузку:
          - если включён минимальный режим (self.minimal=True) и прогресса нет — heartbeat пропускается;
          - троттлинг по времени: если с последнего успешного heartbeat прошло меньше
            self.hb_min_interval секунд и прогресса нет — пропускаем запись, чтобы не бить по БД.
        """
        rid = run_id or self._current_run_id
        if not rid:
            return

        # пропуск при «минимальном журнале», когда нет прогресса
        if self.minimal and not progress:
            return

        now_mono = time.monotonic()
        if self.hb_min_interval > 0 and not progress:
            elapsed = now_mono - self._last_hb_mono
            if elapsed < self.hb_min_interval:
                return

        upd = {"heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
        if progress:
            upd.update(progress)

        self.pg.execute(
            f"UPDATE {self.table} SET details = COALESCE(details,'{{}}'::jsonb) || %s::jsonb WHERE id=%s AND status='running' AND ts_end IS NULL",
            (Json(upd), rid)
        )
        # после успешного апдейта фиксируем «последний heartbeat» для троттлинга
        self._last_hb_mono = now_mono

        try:
            # Агрегированное состояние обновляем только «лёгкими» полями: heartbeat и прогресс.
            self._state_upsert(
                last_heartbeat=datetime.now(timezone.utc),
                progress=progress or None
            )
        except Exception:
            pass

    def mark_done(self, slice_from: datetime, slice_to: datetime, rows_read: int, rows_written: int) -> None:
        """
        Завершает текущий срез (status='ok'), пишет фактические метрики и обновляет watermark правой границей среза.
        slice_from/slice_to трактуются как UTC.
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

        # Успешное завершение: обновляем агрегированное состояние и водяной знак.
        try:
            self._state_upsert(
                status="ok",
                healthy=True,
                last_ok_end=self._to_aware_utc(slice_to),
                last_heartbeat=datetime.now(timezone.utc),
                progress={"rows_read": int(rows_read), "rows_written": int(rows_written)}
            )
        except Exception:
            pass

        # Опциональный авто‑прон (ретенция журнала) — ровно один раз в сутки (UTC).
        # Включается переменной окружения JOURNAL_AUTOPRUNE_ON_DONE=1 (по умолчанию включено).
        # Если сегодня уже чистили (флажок в state.extra), повторно не запускаем.
        try:
            self._maybe_autoprune_on_done()
        except Exception:
            # Любые сбои ретенции не должны влиять на основной пайплайн.
            log.warning("auto-prune: ошибка при фоновом запуске ретенции, будет проигнорирована.", exc_info=True)
    def _maybe_autoprune_on_done(self) -> None:
        """
        Опционально запускает мягкую ретенцию журнала (prune_old) один раз в сутки (UTC).
        Управляется переменными окружения:
          - JOURNAL_AUTOPRUNE_ON_DONE = 1|true|yes|on (включено по умолчанию), чтобы не плодить сервисов/cron;
          - JOURNAL_PRUNE_BATCH_SIZE  = число (по умолчанию 20000) — размер батча DELETE;
          - JOURNAL_RETENTION_DAYS    = число (по умолчанию 30) — сколько дней хранить.
        Флажок «уже чистили сегодня» хранится в {state_table}.extra -> 'autoprune_last_done' (строка YYYY-MM-DD, UTC день).
        Любые ошибки ретенции логируются и не прерывают основной поток.
        """
        # Флаг включения через ENV (по умолчанию — включено).
        env = os.getenv("JOURNAL_AUTOPRUNE_ON_DONE", "").strip().lower()
        enabled = (env in ("", "1", "true", "yes", "on"))
        if not enabled:
            return

        # Проверка «чистили ли уже сегодня».
        today = datetime.now(timezone.utc).date().isoformat()
        self.pg.execute(f"SELECT extra->>'autoprune_last_done' FROM {self.state_table} WHERE process_name=%s", (self.process_name,))
        row = self.pg.fetchone()
        last_done = row[0] if row else None
        if last_done == today:
            return  # уже чистили сегодня — выходим тихо

        # Параметры батча: берём из ENV, но с безопасными дефолтами.
        try:
            batch_size = int(os.getenv("JOURNAL_PRUNE_BATCH_SIZE", "20000"))
        except Exception:
            batch_size = 20000

        # Запускаем мягкую ретенцию. days берётся внутри prune_old() из JOURNAL_RETENTION_DAYS (или 30 по умолчанию).
        try:
            deleted = self.prune_old(days=None, batch_size=batch_size)
        except Exception as e:
            log.warning("auto-prune: ошибка prune_old(): %s", e, exc_info=True)
            return

        # Отмечаем в aggregated-state, что сегодня чистка отработала (и сколько строк удалено).
        # ВАЖНО: делаем merge JSONB, чтобы не терять прочие ключи в extra.
        self.pg.execute(f"""
        INSERT INTO {self.state_table} AS s (process_name, extra)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (process_name) DO UPDATE SET
            extra = COALESCE(s.extra, '{{}}'::jsonb) || EXCLUDED.extra,
            updated_at = now()
        """, (self.process_name, Json({"autoprune_last_done": today, "autoprune_deleted": int(deleted)})))
        # Коммитим апдейт aggregated-state; вместе с ним зафиксируются и DELETE из prune_old() (если автокоммит выключен).
        try:
            self.pg.commit()
        except Exception:
            pass

        log.info("auto-prune: выполнено за текущие сутки (UTC=%s), удалено %d записей.", today, int(deleted))

    def mark_error(self, slice_from: datetime, slice_to: datetime, message: str) -> None:
        """Фиксирует ошибку для текущего среза (status='error') без изменения watermark.
        slice_from/slice_to трактуются как UTC.
        """
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
        try:
            self._state_upsert(
                status="error",
                healthy=False,
                last_error_at=datetime.now(timezone.utc),
                last_error_component="runtime",
                last_error_message=str(message)
            )
        except Exception:
            pass

    def mark_startup_error(
        self,
        message: str,
        *,
        component: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        host: Optional[str] = None,
        pid: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Фиксирует «стартовую» ошибку процесса (до начала обработки срезов).
        Используйте, когда упали подключения к внешним БД/сервисам, конфиг невалиден и т.п.

        Поведение:
          - создаётся отдельная строка в журнале со status='error' и ts_end=now();
          - запись НЕ активная (ts_end не NULL), поэтому не блокирует planned/running;
          - в details пишется phase='bootstrap', error, component и при наличии границы запуска.

        Аргументы:
          message   — человекочитаемое сообщение об ошибке.
          component — источник сбоя: 'phoenix' | 'clickhouse' | 'postgres' | 'config' | и т.п.
          since/until — опциональные рамки бизнес-запуска, если известны на момент сбоя.
          host/pid  — опциональные идентификаторы узла/процесса.
          extra     — дополнительные поля, будут добавлены в details (значения приводятся к str при необходимости).

        Возвращает:
          id — идентификатор созданной строки журнала.
        """
        det: Dict[str, Any] = {"phase": "bootstrap", "error": str(message)}
        if component:
            det["component"] = component
        if since:
            det["slice_from"] = self._to_aware_utc(since).isoformat()
        if until:
            det["slice_to"] = self._to_aware_utc(until).isoformat()
        if extra:
            # Аккуратно приводим небезопасные для JSONB типы к строке.
            try:
                det.update({k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v)) for k, v in extra.items()})
            except Exception:
                pass

        self.pg.execute(
            f"INSERT INTO {self.table} (process_name, status, ts_end, details, host, pid) "
            "VALUES (%s, 'error', now(), %s::jsonb, %s, %s) RETURNING id",
            (self.process_name, Json(det), host, pid)
        )
        rid = self.pg.fetchone()[0]
        log.error("Стартовый сбой процесса '%s': id=%s, component=%s, message=%s",
                  self.process_name, rid, component or "-", message)
        # Синхронизируем агрегированное состояние: фатальный стартовый сбой.
        try:
            self._state_upsert(
                status="error",
                healthy=False,
                last_error_at=datetime.now(timezone.utc),
                last_error_component=component or "bootstrap",
                last_error_message=message,
                extra=extra or {},
            )
        except Exception:
            pass
        return rid

    # -------------------- watermark и вспомогательные --------------------

    def get_watermark(self) -> Optional[datetime]:
        """Текущий watermark для процесса (UTC) или None, если ещё не выставлялся."""
        self.pg.execute(f"SELECT last_ok_end FROM {self.state_table} WHERE process_name=%s", (self.process_name,))
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

    def _state_upsert(
        self,
        *,
        status: Optional[str] = None,
        healthy: Optional[bool] = None,
        last_ok_end: Optional[datetime] = None,
        last_started_at: Optional[datetime] = None,
        last_heartbeat: Optional[datetime] = None,
        last_error_at: Optional[datetime] = None,
        last_error_component: Optional[str] = None,
        last_error_message: Optional[str] = None,
        progress: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Идемпотентный апсерт агрегированного состояния процесса.
        last_ok_end только растёт (GREATEST()), при 'ok' очищаем поля ошибки.
        """
        now = datetime.now(timezone.utc)
        msg = (last_error_message or None)
        if msg and len(msg) > 1000:
            msg = msg[:1000]

        sql = f"""
        INSERT INTO {self.state_table} AS s
            (process_name, last_status, healthy, last_ok_end, last_started_at, last_heartbeat,
             last_error_at, last_error_component, last_error_message, progress, extra, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, now())
        ON CONFLICT (process_name) DO UPDATE SET
            last_status     = COALESCE(EXCLUDED.last_status, s.last_status),
            healthy         = COALESCE(EXCLUDED.healthy, s.healthy),
            last_ok_end     = GREATEST(COALESCE(s.last_ok_end, EXCLUDED.last_ok_end), EXCLUDED.last_ok_end),
            last_started_at = COALESCE(EXCLUDED.last_started_at, s.last_started_at),
            last_heartbeat  = COALESCE(EXCLUDED.last_heartbeat,  now()),
            last_error_at        = CASE WHEN EXCLUDED.last_status = 'ok' THEN NULL ELSE COALESCE(EXCLUDED.last_error_at, s.last_error_at) END,
            last_error_component = CASE WHEN EXCLUDED.last_status = 'ok' THEN NULL ELSE COALESCE(EXCLUDED.last_error_component, s.last_error_component) END,
            last_error_message   = CASE WHEN EXCLUDED.last_status = 'ok' THEN NULL ELSE COALESCE(EXCLUDED.last_error_message, s.last_error_message) END,
            progress        = COALESCE(EXCLUDED.progress, s.progress),
            extra           = COALESCE(EXCLUDED.extra, s.extra),
            updated_at      = now()
        """
        self.pg.execute(sql, (
            self.process_name,
            status, healthy, last_ok_end, last_started_at,
            last_heartbeat or now,
            last_error_at, last_error_component, msg,
            json.dumps(progress) if progress is not None else None,
            json.dumps(extra) if extra is not None else None,
        ))
        self.pg.commit()