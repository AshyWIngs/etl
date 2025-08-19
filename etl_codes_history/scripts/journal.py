# file: scripts/journal.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import logging
from typing import Any, Dict, Optional, List, Tuple, Protocol, runtime_checkable, Callable, cast
from datetime import datetime, timezone, timedelta, tzinfo
from contextlib import contextmanager
import os
import time
import re
try:
    from psycopg.types.json import Json  # type: ignore[import]
except Exception:
    # Fallback stub for type-checkers / dev environments without psycopg
    def Json(obj):  # type: ignore[misc]
        return obj
try:
    from psycopg.errors import UniqueViolation  # type: ignore[import]
except Exception:
    # Fallback stub exception to satisfy type-checkers
    class UniqueViolation(Exception):  # type: ignore[misc]
        ...

# Настройка таймзоны вывода для ЛОГОВ (не влияет на UTC в БД, отображение через CLI)
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # type: ignore

_LOG_TZ_CONFIGURED = False

# Предкомпилированный шаблон для разбора границ партиций: быстрее, чем компилировать в цикле каждый раз
_RX_PART_BOUND = re.compile(r"FROM \('(.*?)'\) TO \('(.*?)'\)")

# Предкомпилированные шаблоны для быстрой нормализации оффсетов таймзоны

_RX_TZ_OFF_HH   = re.compile(r'([+\-]\d{2})$')   # '+05' / '-03' в конце строки
_RX_TZ_OFF_HHMM = re.compile(r'([+\-]\d{4})$')   # '+0530' / '-0330' в конце строки

# --- Узкие протоколы для подсказок типизации (Pylance/pyright) ---
@runtime_checkable
class _HasExecute(Protocol):
    def execute(self, sql: str, params: Any = ...) -> Any: ...

@runtime_checkable
class _HasClose(Protocol):
    def close(self) -> Any: ...

@runtime_checkable
class _HasFetchone(Protocol):
    def fetchone(self) -> Optional[tuple]: ...

@runtime_checkable
class _HasFetchall(Protocol):
    def fetchall(self) -> List[tuple]: ...

@runtime_checkable
class _CursorLikeProto(_HasExecute, _HasClose, Protocol):
    ...

@runtime_checkable
class _HasCursor(Protocol):
    def cursor(self) -> "_CursorLikeProto": ...

@runtime_checkable
class _HasCommit(Protocol):
    def commit(self) -> Any: ...

@runtime_checkable
class _PgLikeProto(_HasExecute, _HasFetchone, _HasFetchall, _HasCursor, _HasCommit, Protocol):
    ...

class _TzFormatter(logging.Formatter):
    """
    Форматтер, печатающий %(asctime)s в заданной таймзоне.
    Время берём как UTC и переводим в нужный TZ только для логов.
    """
    def __init__(self, fmt: str | None = None, datefmt: str | None = None, tzinfo: tzinfo | None = None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._tz = tzinfo or timezone.utc

    def formatTime(self, record, datefmt: str | None = None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone(self._tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="seconds")

def _resolve_log_timezone(name: str) -> tzinfo:
    """Подбирает tzinfo по имени зоны; если не найдено — fallback на GMT+5."""
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone(timedelta(hours=5))  # Asia/Almaty

def _configure_logger_timezone_once() -> None:
    """
    Один раз перестраивает форматтеры хэндлеров текущего логгера так,
    чтобы время в логах печаталось в бизнес-таймзоне.
    Управляется переменной окружения JOURNAL_LOG_TZ (рекомендуется 'Asia/Almaty').
    """
    global _LOG_TZ_CONFIGURED
    if _LOG_TZ_CONFIGURED:
        return

    tz_name = (os.getenv("JOURNAL_LOG_TZ") or "Asia/Almaty").strip()
    tzinfo = _resolve_log_timezone(tz_name)

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
        # Единый компактный формат логов без выравнивания уровня (убираем лишние пробелы у INFO)
        cur_fmt = os.getenv("JOURNAL_LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        cur_datefmt = os.getenv("JOURNAL_LOG_DATEFMT", "%Y-%m-%d %H:%M:%S")
        h.setFormatter(_TzFormatter(fmt=cur_fmt, datefmt=cur_datefmt, tzinfo=tzinfo))

    _LOG_TZ_CONFIGURED = True

log = logging.getLogger("scripts.journal")


class ProcessJournal:
    """
    Журнал инкрементальных запусков + watermark в PostgreSQL.

    Минимально и стабильно:
      * <schema>.inc_processing      — журнал запусков (planned|running|ok|error|skipped), PARTITION BY RANGE (ts_start)
      * <schema>.inc_process_state   — агрегированное состояние (watermark, hb, последний статус)

    Основные принципы:
      - В один момент времени может быть только ОДНА «активная» запись по процессу
        (status in ('planned','running') and ts_end is null) — это гарантирует частичный UNIQUE индекс на дочках.
      - Переход planned→running выполняется атомарно (UPDATE по найденному planned).
      - Все метаданные и показатели складываются в JSONB `details`.
      - Watermark — правая граница обработанного бизнес-интервала (UTC).
      - Для фатальных стартовых сбоев предусмотрена запись ошибки без привязки к окну — mark_startup_error().

    Режим «минимального журнала» и heartbeat-троттлинг:
      - Можно уменьшить число записей в БД: писать только planned → running → ok/error (без пустых heartbeat).
      - Порог частоты heartbeat регулируется JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC (по умолчанию 30 сек).

    Часовой пояс ЛОГОВ:
      - JOURNAL_LOG_TZ (по умолчанию 'Asia/Almaty') влияет только на печать времени в логах.
      - Все значения времени, которые пишутся в БД (журнал/состояние/метки sanitized_at), — строго в UTC.

    ENV:
      - JOURNAL_PARTITION_INTERVAL_DAYS=7|14|30 (def: 7) — длина окна партиции
      - JOURNAL_PARTITION_BEHIND=1, JOURNAL_PARTITION_AHEAD=1 — количество окон назад/вперёд
      - JOURNAL_LOG_TZ='Asia/Almaty' — TZ только для логов
    """

    # ---------- PG commit helper ----------

    def _commit_quietly(self) -> None:
        """
        Best-effort commit для разных PG-обёрток.
        Если .commit() нет — предполагаем autocommit. Исключения гасим.
        """
        try:
            commit_fn = getattr(self.pg, "commit", None)
            if callable(commit_fn):
                commit_fn()
                return
            conn = getattr(self.pg, "conn", None) or getattr(self.pg, "connection", None)
            if conn and hasattr(conn, "commit"):
                conn.commit()
        except Exception:
            pass

    def _self_check_pg_client(self) -> None:
        """
        Разовый self-check PG‑клиента:
          - если у обёртки нет .commit() и нет .conn/.connection.commit(),
            выводим предупреждение (считаем, что autocommit).
          - это чисто диагностическое сообщение для раннего выявления «сырых» клиентов.
        """
        try:
            has_commit = callable(getattr(self.pg, "commit", None))
            if not has_commit:
                conn = getattr(self.pg, "conn", None) or getattr(self.pg, "connection", None)
                if conn and hasattr(conn, "commit"):
                    has_commit = True
            if not has_commit:
                log.warning(
                    "ProcessJournal: предоставленный PG‑клиент не имеет .commit() "
                    "или .connection.commit(). Предполагаю autocommit; транзакции "
                    "будут зависеть от настроек драйвера/сервера."
                )
        except Exception:
            # Диагностика не должна ломать основной поток.
            pass

    def __init__(self, pg: _PgLikeProto, table: str, process_name: str, state_table: Optional[str] = None,
                 minimal: Optional[bool] = None, heartbeat_min_interval_sec: Optional[int] = None):
        _configure_logger_timezone_once()
        self.pg: _PgLikeProto = pg  # типизация для Pylance: есть execute/fetchone/fetchall/cursor/commit
        self.table = table
        self.process_name = process_name
        self.state_table = state_table or self._derive_state_table_name(table)
        self._current_run_id: Optional[int] = None
        self._lock_acquired: bool = False

        # Минимальный режим журналирования жёстко включён:
        # пишем только planned → running → ok/error. Heartbeat отключён полностью.
        self.minimal: bool = True
        try:
            self.hb_min_interval = int(os.getenv("JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", "300"))
        except Exception:
            self.hb_min_interval = 300
        self._last_hb_mono: float = 0.0
        self._self_check_pg_client()

        # Кэш разбора имён и relkind для снижения накладных расходов на повторные SELECT и split()
        self._schema, self._parent_name = self._split_schema_table(self.table)
        self._tail_table = self._tail_identifier(self.table)
        self._state_schema, self._state_name = self._split_schema_table(self.state_table)
        self._tail_state_table = self._tail_identifier(self.state_table)
        self._relkind_cache: Optional[str] = None  # 'p' — партиционированная, 'r' — обычная, None — неизвестно

    # ---------- утилиты имён/схем ----------

    @staticmethod
    def _tail_identifier(ident: str) -> str:
        tail = ident.split(".")[-1]
        return tail.strip('"')

    @staticmethod
    def _derive_state_table_name(journal_table: str) -> str:
        parts = journal_table.split(".")
        return f"{parts[0]}.inc_process_state" if len(parts) == 2 else "inc_process_state"

    @staticmethod
    def _split_schema_table(fqname: str) -> Tuple[str, str]:
        parts = fqname.split(".")
        if len(parts) == 2:
            return parts[0], parts[1]
        return "public", parts[0]

    def _table_relkind(self) -> Optional[str]:
        """'p' — партиционированная родительская таблица, 'r' — обычная, None — таблицы нет. Использует кэш."""
        # Быстрый путь: если уже знаем тип — возвращаем без запроса к каталогу
        if getattr(self, "_relkind_cache", None) is not None:
            return self._relkind_cache
        # Медленный путь: один раз читаем из системного каталога
        schema = getattr(self, "_schema", None) or self._split_schema_table(self.table)[0]
        name = getattr(self, "_parent_name", None) or self._split_schema_table(self.table)[1]
        self.pg.execute(
            "SELECT c.relkind "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s",
            (schema, name)
        )
        row = self.pg.fetchone()
        self._relkind_cache = row[0] if row else None
        return self._relkind_cache

    def _is_parent_partitioned(self) -> bool:
        return self._table_relkind() == "p"

    # ---------- расчёт партиций ----------

    @staticmethod
    def _floor_to_interval_utc(dt: datetime, days: int) -> datetime:
        """
        Округляет вниз до границы интервала (7/14/30 суток), якорь — понедельник 1970-01-05.
        Это даёт стабильные окна, начинающиеся по понедельникам.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        anchor = datetime(1970, 1, 5, tzinfo=timezone.utc)  # Monday
        delta_days = (dt.date() - anchor.date()).days
        step = (delta_days // days) * days
        start = anchor + timedelta(days=step)
        return start

    @staticmethod
    def _fmt_ts(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

    def _norm_tz_offset_str(self, s: str) -> str:
        """
        Нормализует строку времени с оффсетом зоны:
          • '+05'    → '+05:00'
          • '-03'    → '-03:00'
          • '+0530'  → '+05:30'
          • '-0330'  → '-03:30'
        Остальные варианты возвращаются без изменений.
        """
        if not isinstance(s, str):
            return s
        # '+HH' или '-HH' в конце
        m = _RX_TZ_OFF_HH.search(s)
        if m:
            off = m.group(1)
            return s[:-len(off)] + off + ":00"
        # '+HHMM' или '-HHMM' в конце
        m = _RX_TZ_OFF_HHMM.search(s)
        if m:
            off = m.group(1)
            return s[:-len(off)] + off[:3] + ":" + off[3:]
        return s

    def _parse_ts_any(self, text: str) -> datetime:
        """
        Парсит timestamptz в Python datetime с поддержкой оффсетов '+05' / '+0530'.
        Возвращает aware-UTC datetime.
        """
        s = self._norm_tz_offset_str(text)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _ensure_partitions_around_now(self) -> None:
        """
        Если журнал — партиционированный, создаёт недостающие партиции вокруг «сейчас»:
          - N прошлых окон (JOURNAL_PARTITION_BEHIND, дефолт 1),
          - текущее окно,
          - N будущих окон (JOURNAL_PARTITION_AHEAD, дефолт 1).

        Длина окна — JOURNAL_PARTITION_INTERVAL_DAYS (7 по умолчанию; допускаются 7/14/30).

        Оптимизации:
          • все DDL/CREATE INDEX выполняем через один курсор (если self.pg поддерживает .cursor()),
            чтобы сократить накладные расходы на round-trip;
          • один общий commit в конце (best‑effort через _commit_quietly()).
        """
        try:
            if not self._is_parent_partitioned():
                return

            try:
                interval_days = int(os.getenv("JOURNAL_PARTITION_INTERVAL_DAYS", "7"))
            except Exception:
                interval_days = 7
            if interval_days not in (7, 14, 30):
                interval_days = 7

            try:
                behind = max(0, int(os.getenv("JOURNAL_PARTITION_BEHIND", "1")))
            except Exception:
                behind = 1
            try:
                ahead = max(0, int(os.getenv("JOURNAL_PARTITION_AHEAD", "1")))
            except Exception:
                ahead = 1

            schema, parent = self._schema, self._parent_name
            now_utc = datetime.now(timezone.utc)
            cur_start = self._floor_to_interval_utc(now_utc, interval_days)

            def part_name(st: datetime, en: datetime) -> str:
                return f"{parent}_p_{st.strftime('%Y%m%d')}_{en.strftime('%Y%m%d')}"

            def _fmt(dt: datetime) -> str:
                return self._fmt_ts(dt)

            # Выбираем API выполнения: один курсор, если доступен
            cur: Optional[_CursorLikeProto] = None
            try:
                cur_factory = getattr(self.pg, "cursor", None)
                if callable(cur_factory):
                    cur = cast(_CursorLikeProto, cur_factory())
                    _exec: Callable[..., Any] = cast(_HasExecute, cur).execute
                else:
                    _exec: Callable[..., Any] = cast(_HasExecute, self.pg).execute

                def create_one(st: datetime) -> None:
                    en = st + timedelta(days=interval_days)
                    child = part_name(st, en)
                    st_s = _fmt(st)
                    en_s = _fmt(en)
                    # 1) партиция
                    _exec(
                        f"CREATE TABLE IF NOT EXISTS {schema}.{child} PARTITION OF {self.table} "
                        f"FOR VALUES FROM ('{st_s}') TO ('{en_s}')"
                    )
                    # 2) индексы: активная уникальность + быстрый выбор завершённых
                    ix_active = f"{child}_active_one_uq_idx"
                    ix_ended  = f"{child}_ended_notnull_idx"
                    _exec(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS {ix_active} "
                        f"ON {schema}.{child} (process_name) "
                        f"WHERE ts_end IS NULL AND status IN ('planned','running')"
                    )
                    _exec(
                        f"CREATE INDEX IF NOT EXISTS {ix_ended} "
                        f"ON {schema}.{child} (ts_end) WHERE ts_end IS NOT NULL"
                    )

                for i in range(behind, 0, -1):
                    create_one(cur_start - timedelta(days=i * interval_days))
                create_one(cur_start)
                for i in range(1, ahead + 1):
                    create_one(cur_start + timedelta(days=i * interval_days))
            finally:
                try:
                    if cur is not None:
                        cur.close()
                except Exception:
                    pass

            self._commit_quietly()
        except Exception:
            log.warning("ensure_partitions_around_now(): не удалось создать/индексировать партиции.", exc_info=True)

    def _list_partitions_with_bounds(self) -> List[Tuple[str, datetime, datetime]]:
        """[(child_relname, from_ts, to_ts)] по pg_inherits/pg_get_expr()."""
        schema, parent = self._schema, self._parent_name
        self.pg.execute(
            "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) "
            "FROM pg_inherits i "
            "JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_class p ON p.oid = i.inhparent "
            "JOIN pg_namespace n ON n.oid = p.relnamespace "
            "WHERE n.nspname=%s AND p.relname=%s "
            "ORDER BY c.relname",
            (schema, parent)
        )
        rows = self.pg.fetchall() or []
        out: List[Tuple[str, datetime, datetime]] = []
        for relname, bound in rows:
            m = _RX_PART_BOUND.search(bound or "")
            if not m:
                continue
            try:
                st = self._parse_ts_any(m.group(1))
                en = self._parse_ts_any(m.group(2))
                out.append((relname, st, en))
            except Exception:
                # Если не смогли распарсить — пропускаем партицию, но не падаем
                log.debug("Не удалось разобрать границы партиции %s: %s", relname, bound, exc_info=True)
        return out
    def resolve_active_conflicts_for_slice(self, slice_from: datetime, slice_to: datetime, keep_run_id: Optional[int] = None) -> None:
        """
        Расклеивает гонки: для того же бизнес-окна переводит чужие активные записи
        (planned|running, ts_end IS NULL) в безопасные статусы.
          • planned  -> skipped
          • running  -> error (ts_end=now())
        keep_run_id — запись, которую сохраняем активной (если известна).
        """
        sf, st = self._slice_iso_texts(slice_from, slice_to)
        # planned -> skipped
        sql_pl = f"""
        UPDATE {self.table}
           SET status='skipped'
         WHERE process_name=%s
           AND status='planned'
           AND ts_end IS NULL
           AND (details->>'slice_from')=%s
           AND (details->>'slice_to')=%s
           { "AND id<>%s" if keep_run_id is not None else "" }
        """
        params_pl: List[Any] = [self.process_name, sf, st]
        if keep_run_id is not None:
            params_pl.append(keep_run_id)
        self.pg.execute(sql_pl, tuple(params_pl))

        # running -> error
        sql_ru = f"""
        UPDATE {self.table}
           SET status='error',
               ts_end=now()
         WHERE process_name=%s
           AND status='running'
           AND ts_end IS NULL
           AND (details->>'slice_from')=%s
           AND (details->>'slice_to')=%s
           { "AND id<>%s" if keep_run_id is not None else "" }
        """
        params_ru: List[Any] = [self.process_name, sf, st]
        if keep_run_id is not None:
            params_ru.append(keep_run_id)
        self.pg.execute(sql_ru, tuple(params_ru))
        self._commit_quietly()

    def _prune_by_partitions(self, days: int) -> int:
        """
        Ретенция партициями: DETACH + DROP дочерних секций, верхняя граница которых
        строго меньше (now() - days).
        """
        if days <= 0:
            return 0
        schema = self._schema
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
        parts = self._list_partitions_with_bounds()
        victims = [(name, st, en) for (name, st, en) in parts if en < cutoff]
        if not victims:
            return 0

        # Всегда берём advisory-lock: один инстанс — одна чистка партиций.
        self.pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", ("journal_prune_global",))
        _row_lock = self.pg.fetchone()
        got = bool(_row_lock[0]) if _row_lock else False  # без индексации None
        if not got:
            log.info("prune_by_partitions: другой инстанс уже чистит — выходим.")
            return 0
        try:
            dropped = 0
            for child, st, en in victims:
                self.pg.execute(f"ALTER TABLE {self.table} DETACH PARTITION {schema}.{child}")
                self.pg.execute(f"DROP TABLE IF EXISTS {schema}.{child}")
                dropped += 1
            self._commit_quietly()
            if dropped:
                log.warning("prune_by_partitions(days=%d): удалено партиций: %d.", days, dropped)
            return dropped
        finally:
            self.pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", ("journal_prune_global",))

    # ---------- ensure() с режимом «партиции по умолчанию» ----------
    def ensure(self) -> None:
        """
        Создаёт таблицы и индексы, если их нет (идемпотентно).
        СТРОГИЙ режим: поддерживается ТОЛЬКО партиционированный журнал (PARTITION BY RANGE(ts_start)).
        Если существует непартиционированная таблица — возбуждаем RuntimeError и просим миграцию.

        Оптимизация: все DDL внутри выполняем через ОДИН курсор (если у PG‑клиента есть .cursor()),
        чтобы сократить число round‑trip в БД; в конце — один общий commit (best‑effort).
        """
        relkind = self._table_relkind()

        # Выбираем API выполнения: один курсор, если доступен
        cur: Optional[_CursorLikeProto] = None
        try:
            cur_factory = getattr(self.pg, "cursor", None)
            if callable(cur_factory):
                cur = cast(_CursorLikeProto, cur_factory())
                _exec: Callable[..., Any] = cast(_HasExecute, cur).execute
            else:
                _exec: Callable[..., Any] = cast(_HasExecute, self.pg).execute

            if relkind is None:
                # Родительской таблицы ещё нет — создаём и сразу нужный индекс
                _exec(f"""
                CREATE TABLE {self.table} (
                    id            BIGINT GENERATED BY DEFAULT AS IDENTITY,
                    process_name  TEXT        NOT NULL,
                    ts_start      TIMESTAMPTZ NOT NULL DEFAULT now(),
                    ts_end        TIMESTAMPTZ NULL,
                    status        TEXT        NOT NULL CHECK (status IN ('planned','running','ok','error','skipped')),
                    details       JSONB       NULL,
                    host          TEXT        NULL,
                    pid           INTEGER     NULL
                ) PARTITION BY RANGE (ts_start);
                """)
                _exec(f"""
                CREATE INDEX IF NOT EXISTS {self._tail_table}_pname_started_desc_idx
                  ON {self.table} (process_name, ts_start DESC);
                """)
                log.info("Проверка таблицы журнала: OK (partitioned, %s)", self.table)
                # Кешируем тип, чтобы ниже не делать повторные проверки
                self._relkind_cache = "p"

                # Дочерние партиции и ретенция — отдельными вызовами (они сами используют единый commit)
                self._ensure_partitions_around_now()
                self._auto_prune_if_due()

            elif relkind == "p":
                # Таблица уже партиционированная — убеждаемся, что есть нужный индекс
                log.info("Проверка таблицы журнала: OK (partitioned, %s)", self.table)
                self._relkind_cache = "p"
                _exec(f"""
                CREATE INDEX IF NOT EXISTS {self._tail_table}_pname_started_desc_idx
                  ON {self.table} (process_name, ts_start DESC);
                """)
                self._ensure_partitions_around_now()
                self._auto_prune_if_due()

            else:
                # Если открыли курсор — аккуратно закроем его перед исключением
                raise RuntimeError(
                    f"Журнал {self.table} непартиционирован (relkind={relkind}). "
                    "Требуется миграция на PARTITION BY RANGE(ts_start)."
                )

            # STATE — создаём/добавляем индексы тем же курсором
            _exec(f"""
            CREATE TABLE IF NOT EXISTS {self.state_table} (
                process_name          TEXT PRIMARY KEY,
                last_status           TEXT,
                healthy               BOOLEAN,
                last_ok_end           TIMESTAMPTZ,
                last_started_at       TIMESTAMPTZ,
                last_heartbeat        TIMESTAMPTZ,
                last_error_at         TIMESTAMPTZ,
                last_error_component  TEXT,
                last_error_message    TEXT,
                progress              JSONB,
                extra                 JSONB,
                updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
            _exec(f"CREATE INDEX IF NOT EXISTS {self._tail_state_table}_updated_at_idx ON {self.state_table}(updated_at DESC);")
            _exec(f"CREATE INDEX IF NOT EXISTS {self._tail_state_table}_last_ok_end_idx ON {self.state_table}(last_ok_end);")
            log.info("Проверка таблицы состояния: OK (%s)", self.state_table)

        finally:
            # Закрываем курсор, если создавали, и фиксируем все DDL одним коммитом (best‑effort)
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            self._commit_quietly()
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
        Надёжный UPSERT агрегированного состояния процесса в inc_process_state.

        Важно:
          • Только позиционные параметры (%s), без именованных плейсхолдеров — совместимо с любыми простыми обёртками.
          • Все timestamptz передаются в виде ISO‑строк в UTC и приводятся в SQL с ::timestamptz.
          • progress/extra пишем как JSONB; extra при UPSERT аккуратно мёрджим (old.extra || new.extra).
        """
        # Собираем полезную нагрузку: пишем только непустые поля
        payload: Dict[str, Any] = {}

        if status is not None:
            payload["last_status"] = str(status)
        if healthy is not None:
            payload["healthy"] = bool(healthy)

        def _iso(v: Optional[datetime]) -> Optional[str]:
            if v is None:
                return None
            dv = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
            return dv.astimezone(timezone.utc).isoformat()

        if last_ok_end is not None:
            payload["last_ok_end"] = _iso(last_ok_end)
        if last_started_at is not None:
            payload["last_started_at"] = _iso(last_started_at)
        if last_heartbeat is not None:
            payload["last_heartbeat"] = _iso(last_heartbeat)
        if last_error_at is not None:
            payload["last_error_at"] = _iso(last_error_at)

        if last_error_component is not None:
            payload["last_error_component"] = str(last_error_component)
        if last_error_message is not None:
            payload["last_error_message"] = str(last_error_message)

        if progress:
            payload["progress"] = progress
        if extra:
            payload["extra"] = extra

        if not payload:
            return

        # Извлекаем значения по ключам; отсутствующие оставляем None (INSERT пройдёт с NULL'ами)
        vals = {
            "last_status":          payload.get("last_status"),
            "healthy":              payload.get("healthy"),
            "last_ok_end":          payload.get("last_ok_end"),
            "last_started_at":      payload.get("last_started_at"),
            "last_heartbeat":       payload.get("last_heartbeat"),
            "last_error_at":        payload.get("last_error_at"),
            "last_error_component": payload.get("last_error_component"),
            "last_error_message":   payload.get("last_error_message"),
            "progress":             payload.get("progress") or {},
            "extra":                payload.get("extra") or {},
        }

        # Единый UPSERT без именованных плейсхолдеров
        sql = f"""
            INSERT INTO {self.state_table} (
                process_name,
                last_status,
                healthy,
                last_ok_end,
                last_started_at,
                last_heartbeat,
                last_error_at,
                last_error_component,
                last_error_message,
                progress,
                extra,
                updated_at
            ) VALUES (
                %s,  -- process_name
                %s,  -- last_status
                %s,  -- healthy
                %s::timestamptz,  -- last_ok_end
                %s::timestamptz,  -- last_started_at
                %s::timestamptz,  -- last_heartbeat
                %s::timestamptz,  -- last_error_at
                %s,  -- last_error_component
                %s,  -- last_error_message
                %s::jsonb,  -- progress
                %s::jsonb,  -- extra
                now()
            )
            ON CONFLICT (process_name) DO UPDATE SET
                last_status          = COALESCE(EXCLUDED.last_status, {self.state_table}.last_status),
                healthy              = COALESCE(EXCLUDED.healthy, {self.state_table}.healthy),
                last_ok_end          = COALESCE(EXCLUDED.last_ok_end, {self.state_table}.last_ok_end),
                last_started_at      = COALESCE(EXCLUDED.last_started_at, {self.state_table}.last_started_at),
                last_heartbeat       = COALESCE(EXCLUDED.last_heartbeat, {self.state_table}.last_heartbeat),
                last_error_at        = COALESCE(EXCLUDED.last_error_at, {self.state_table}.last_error_at),
                last_error_component = COALESCE(EXCLUDED.last_error_component, {self.state_table}.last_error_component),
                last_error_message   = COALESCE(EXCLUDED.last_error_message, {self.state_table}.last_error_message),
                progress             = COALESCE(EXCLUDED.progress, {self.state_table}.progress),
                extra                = COALESCE({self.state_table}.extra, '{{}}'::jsonb)
                                       || COALESCE(EXCLUDED.extra, '{{}}'::jsonb),
                updated_at           = now()
        """

        params = (
            self.process_name,
            vals["last_status"],
            vals["healthy"],
            vals["last_ok_end"],
            vals["last_started_at"],
            vals["last_heartbeat"],
            vals["last_error_at"],
            vals["last_error_component"],
            vals["last_error_message"],
            Json(vals["progress"]),
            Json(vals["extra"]),
        )

        # Выполняем и фиксируем (best‑effort)
        self.pg.execute(sql, params)
        self._commit_quietly()

    def get_state(self) -> Dict[str, Any]:
        """Возвращает текущую строку из inc_process_state по процессу (или пустой dict)."""
        self.pg.execute(f"SELECT last_status, healthy, last_ok_end, last_started_at, last_heartbeat, progress, extra FROM {self.state_table} WHERE process_name=%s", (self.process_name,))
        row = self.pg.fetchone()
        if not row:
            return {}
        keys = ["last_status", "healthy", "last_ok_end", "last_started_at", "last_heartbeat", "progress", "extra"]
        out = dict(zip(keys, row))
        # Приводим временные поля к строкам ISO (если они datetime)
        for k in ("last_ok_end", "last_started_at", "last_heartbeat"):
            v = out.get(k)
            if isinstance(v, datetime):
                out[k] = (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat()
        return out

    # ---------- утилиты времени ----------

    @staticmethod
    def _to_aware_utc(v: Any) -> datetime:
        if isinstance(v, datetime):
            return (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
        raise TypeError("expected datetime")

    def _slice_iso_texts(self, slice_from: datetime, slice_to: datetime) -> Tuple[str, str]:
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        return sf, st

    # ---------- эксклюзивные блокировки ----------

    def try_acquire_exclusive_lock(self) -> bool:
        self.pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (self.process_name,))
        _row_lock = self.pg.fetchone()
        got = bool(_row_lock[0]) if _row_lock else False  # fetchone() может вернуть None
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

    # ---------- API журнала ----------

    def mark_planned(self, slice_from: datetime, slice_to: datetime) -> int:
        sf = self._to_aware_utc(slice_from).isoformat()
        st = self._to_aware_utc(slice_to).isoformat()
        payload = {"slice_from": sf, "slice_to": st, "planned": True}

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
            _row_new = self.pg.fetchone()
            if _row_new is None:
                # Нестандартно для INSERT ... RETURNING, но явно защищаемся для статической типизации
                raise RuntimeError("INSERT ... RETURNING id не вернул строку с id")
            rid = int(_row_new[0])
            log.info("Запланирован запуск %s: id=%s [%s → %s]", self.process_name, rid, sf, st)
            self._commit_quietly()
            return rid
        except UniqueViolation:
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
        sf, st = self._slice_iso_texts(slice_from, slice_to)
        upd = {"slice_from": sf, "slice_to": st, "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}

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
                self._state_upsert(status="running", healthy=None, last_started_at=datetime.now(timezone.utc))
            except Exception:
                pass
            self._commit_quietly()
            return rid

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
            hb = {"heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
            self.pg.execute(
                f"UPDATE {self.table} SET host=COALESCE(%s,host), pid=COALESCE(%s,pid), details=COALESCE(details,'{{}}'::jsonb)||%s::jsonb WHERE id=%s",
                (host, pid, Json(hb), rid)
            )
            log.info("running (reuse): id=%s [%s → %s]", rid, sf, st)
            self._current_run_id = rid
            try:
                self._state_upsert(status="running", healthy=None, last_started_at=datetime.now(timezone.utc))
            except Exception:
                pass
            self._commit_quietly()
            return rid

        self.resolve_active_conflicts_for_slice(slice_from, slice_to)
        meta = {"slice_from": sf, "slice_to": st, "heartbeat_ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
        try:
            self.pg.execute(
                f"INSERT INTO {self.table} (process_name, status, details, host, pid) VALUES (%s,'running',%s::jsonb,%s,%s) RETURNING id",
                (self.process_name, Json(meta), host, pid)
            )
        except UniqueViolation:
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
                    self._state_upsert(status="running", healthy=None, last_started_at=datetime.now(timezone.utc))
                except Exception:
                    pass
                self._commit_quietly()
                return rid
            raise

        # Безопасно извлекаем результат fetchone() с защитой от None
        _row_run_new = self.pg.fetchone()
        if _row_run_new is None:
            # INSERT ... RETURNING должен вернуть строку; защита от None для статического анализатора
            raise RuntimeError("INSERT ... RETURNING id не вернул строку с id")
        rid = int(_row_run_new[0])
        log.info("running (new): id=%s [%s → %s]", rid, sf, st)
        self._current_run_id = rid
        try:
            self._state_upsert(status="running", healthy=None, last_started_at=datetime.now(timezone.utc))
        except Exception:
            pass
        self._commit_quietly()
        return rid


    def heartbeat(self, run_id: Optional[int] = None, progress: Optional[Dict[str, Any]] = None) -> None:
        """
        «Лёгкий» heartbeat:
          • НЕ пишет новых строк в журнал (inc_processing);
          • обновляет только inc_process_state.last_heartbeat (и progress при наличии);
          • троттлинг: не чаще чем раз в JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC секунд (def: 300).
        """
        now_mono = time.monotonic()
        if now_mono - self._last_hb_mono < max(1, int(self.hb_min_interval)):
            return
        self._last_hb_mono = now_mono
        try:
            self._state_upsert(
                status="running",
                healthy=None,
                last_heartbeat=datetime.now(timezone.utc),
                progress=progress or {}
            )
        except Exception:
            # heartbeat не должен валить основной ETL-поток
            pass

    def sanitize_stale(
        self,
        planned_ttl_minutes: int = 60,
        running_heartbeat_timeout_minutes: int = 45,
        running_hard_ttl_hours: Optional[int] = 12,
    ) -> None:
        """
        «Мягкая» санация висящих запусков ТОЛЬКО для текущего процесса:
          • planned старше planned_ttl_minutes → status='skipped'
          • running без heartbeat дольше running_heartbeat_timeout_minutes → status='error', ts_end=now()
          • (опц.) running старше running_hard_ttl_hours → status='error', ts_end=now()

        Ничего не удаляет — только меняет статусы. Все отметки времени — UTC.
        """
        # planned → skipped (слишком старые planned без завершения)
        sql_planned = f"""
        UPDATE {self.table} t
           SET status = 'skipped'
         WHERE t.process_name = %s
           AND t.status = 'planned'
           AND t.ts_end IS NULL
           AND t.ts_start < now() - INTERVAL '{int(planned_ttl_minutes)} minutes'
        RETURNING t.id
        """
        self.pg.execute(sql_planned, (self.process_name,))
        skipped = len(self.pg.fetchall() or [])

        # running с «протухшим» heartbeat → error
        sql_running_hb = f"""
        UPDATE {self.table} t
           SET status = 'error',
               ts_end = now()
         WHERE t.process_name = %s
           AND t.status = 'running'
           AND t.ts_end IS NULL
           AND COALESCE( (t.details->>'heartbeat_ts')::timestamptz, t.ts_start )
               < now() - INTERVAL '{int(running_heartbeat_timeout_minutes)} minutes'
        RETURNING t.id
        """
        self.pg.execute(sql_running_hb, (self.process_name,))
        hb_err = len(self.pg.fetchall() or [])

        # running старше жёсткого TTL → error
        hard_err = 0
        if running_hard_ttl_hours is not None:
            sql_running_hard = f"""
            UPDATE {self.table} t
               SET status = 'error',
                   ts_end = now()
             WHERE t.process_name = %s
               AND t.status = 'running'
               AND t.ts_end IS NULL
               AND t.ts_start < now() - INTERVAL '{int(running_hard_ttl_hours)} hours'
            RETURNING t.id
            """
            self.pg.execute(sql_running_hard, (self.process_name,))
            hard_err = len(self.pg.fetchall() or [])

        if skipped or hb_err or hard_err:
            log.warning(
                "sanitize_stale(): planned→skipped=%s, running→error (heartbeat)=%s, running→error (hard_ttl)=%s",
                skipped, hb_err, hard_err
            )
        self._commit_quietly()

    def mark_done(
        self,
        slice_from: datetime,
        slice_to: datetime,
        rows_read: int | None = None,
        rows_written: int | None = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """
        Завершить текущий запуск со статусом OK для окна [slice_from, slice_to).
        Ищем «running, ts_end IS NULL» запись именно для этого окна; если вдруг
        не находим — берём последнюю «running» по процессу (safety net).

        Также обновляем агрегированное состояние в inc_process_state:
          • last_status='ok', healthy=true
          • last_ok_end=slice_to (UTC)
          • progress = {rows_read, rows_written}
        """
        sf, st = self._slice_iso_texts(slice_from, slice_to)
        metrics = {
            "rows_read": int(rows_read) if rows_read is not None else None,
            "rows_written": int(rows_written) if rows_written is not None else None,
            "finished": True,
        }
        if extra:
            metrics.update(extra)
        # Убираем None, чтобы не писать null-поля в JSONB
        metrics = {k: v for k, v in metrics.items() if v is not None}

        # 1) Пытаемся закрыть именно запись по нашему окну
        self.pg.execute(
            f"""
            WITH cand AS (
              SELECT id FROM {self.table}
               WHERE process_name=%s AND status='running' AND ts_end IS NULL
                 AND (details->>'slice_from')=%s AND (details->>'slice_to')=%s
               ORDER BY ts_start DESC LIMIT 1
            )
            UPDATE {self.table} t
               SET status='ok',
                   ts_end=now(),
                   details=COALESCE(t.details, '{{}}'::jsonb) || %s::jsonb
              FROM cand
             WHERE t.id=cand.id
            RETURNING t.id
            """,
            (self.process_name, sf, st, Json(metrics))
        )
        row = self.pg.fetchone()

        # 2) Если нет точного совпадения — закрываем последнюю активную запись
        if not row:
            self.pg.execute(
                f"""
                WITH cand AS (
                  SELECT id FROM {self.table}
                   WHERE process_name=%s AND status='running' AND ts_end IS NULL
                   ORDER BY ts_start DESC LIMIT 1
                )
                UPDATE {self.table} t
                   SET status='ok',
                       ts_end=now(),
                       details=COALESCE(t.details, '{{}}'::jsonb) || %s::jsonb
                  FROM cand
                 WHERE t.id=cand.id
                RETURNING t.id
                """,
                (self.process_name, Json(metrics))
            )
            row = self.pg.fetchone()

        rid: Optional[int] = row[0] if row else None
        if rid:
            log.info(
                "Запуск завершён: OK (id=%s) — [%s → %s], rows_read=%s, rows_written=%s",
                rid, sf, st, rows_read, rows_written,
            )
        else:
            log.warning(
                "mark_done(): не нашёл активной записи для завершения [%s → %s] — возможно, гонка/параллелизм",
                sf, st,
            )

        # Best‑effort commit и апдейт состояния
        self._commit_quietly()
        try:
            self._state_upsert(
                status='ok',
                healthy=True,
                last_ok_end=self._to_aware_utc(slice_to),
                progress={k: v for k, v in metrics.items() if k in ("rows_read", "rows_written")},
            )
        except Exception:
            pass

        # Тихо запускаем авто‑ретенцию, если пора
        try:
            self._auto_prune_if_due()
        except Exception:
            pass

        return rid

    def mark_error(
        self,
        slice_from: datetime,
        slice_to: datetime,
        message: str,
        component: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """
        Завершить текущий запуск со статусом ERROR для окна [slice_from, slice_to).
        Пишем ошибку в details (message, component, finished=false) и закрываем запись ts_end=now().
        Обновляем inc_process_state: last_status='error', healthy=false, last_error_*.
        """
        sf, st = self._slice_iso_texts(slice_from, slice_to)
        payload = {"error": str(message), "component": component, "finished": False}
        if extra:
            payload.update(extra)
        payload = {k: v for k, v in payload.items() if v is not None}

        # 1) Попытка по точному окну
        self.pg.execute(
            f"""
            WITH cand AS (
              SELECT id FROM {self.table}
               WHERE process_name=%s AND status IN ('planned','running') AND ts_end IS NULL
                 AND (details->>'slice_from')=%s AND (details->>'slice_to')=%s
               ORDER BY ts_start DESC LIMIT 1
            )
            UPDATE {self.table} t
               SET status='error',
                   ts_end=now(),
                   details=COALESCE(t.details, '{{}}'::jsonb) || %s::jsonb
              FROM cand
             WHERE t.id=cand.id
            RETURNING t.id
            """,
            (self.process_name, sf, st, Json(payload))
        )
        row = self.pg.fetchone()

        # 2) Fallback — последняя активная запись по процессу
        if not row:
            self.pg.execute(
                f"""
                WITH cand AS (
                  SELECT id FROM {self.table}
                   WHERE process_name=%s AND status IN ('planned','running') AND ts_end IS NULL
                   ORDER BY ts_start DESC LIMIT 1
                )
                UPDATE {self.table} t
                   SET status='error',
                       ts_end=now(),
                       details=COALESCE(t.details, '{{}}'::jsonb) || %s::jsonb
                  FROM cand
                 WHERE t.id=cand.id
                RETURNING t.id
                """,
                (self.process_name, Json(payload))
            )
            row = self.pg.fetchone()

        rid: Optional[int] = row[0] if row else None
        if rid:
            log.error("Запуск завершён: ERROR (id=%s) — [%s → %s]: %s", rid, sf, st, message)
        else:
            log.error("mark_error(): не нашёл активной записи для завершения [%s → %s]; error=%s", sf, st, message)

        self._commit_quietly()
        try:
            self._state_upsert(
                status='error',
                healthy=False,
                last_error_at=datetime.now(timezone.utc),
                last_error_component=component or self.process_name,
                last_error_message=str(message),
                extra={},
            )
        except Exception:
            pass
        return rid

    def _auto_prune_if_due(self) -> None:
        """
        Автоматическая ретенция партиций (раз в сутки).
        JOURNAL_RETENTION_DAYS (def: 30) — сколько хранить.
        Метка последней чистки: inc_process_state(process_name='__journal__', extra.last_prune_at).
        """
        try:
            if not self._is_parent_partitioned():
                return
            try:
                days = int(os.getenv("JOURNAL_RETENTION_DAYS", "30"))
            except Exception:
                days = 30
            if days <= 0:
                return

            # Когда чистили в прошлый раз?
            self.pg.execute(f"SELECT extra FROM {self.state_table} WHERE process_name=%s", ("__journal__",))
            row = self.pg.fetchone()
            last_prune_at = None
            if row and row[0]:
                try:
                    last_prune_at_txt = (row[0] or {}).get("last_prune_at")
                    if last_prune_at_txt:
                        last_prune_at = datetime.fromisoformat(last_prune_at_txt)
                        if last_prune_at.tzinfo is None:
                            last_prune_at = last_prune_at.replace(tzinfo=timezone.utc)
                        last_prune_at = last_prune_at.astimezone(timezone.utc)
                except Exception:
                    last_prune_at = None

            now_utc = datetime.now(timezone.utc)
            need = (last_prune_at is None) or ((now_utc - last_prune_at) >= timedelta(days=1))
            if not need:
                return

            # Глобальная защита от гонок
            self.pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", ("journal_prune_global",))
            _row_lock2 = self.pg.fetchone()
            got = bool(_row_lock2[0]) if _row_lock2 else False  # безопасно для pyright/pylance
            if not got:
                return
            try:
                dropped = self._prune_by_partitions(days)
                new_extra = {"last_prune_at": now_utc.isoformat()}
                self.pg.execute(
                    f"""
                    INSERT INTO {self.state_table} (process_name, extra, updated_at)
                    VALUES (%s, %s::jsonb, now())
                    ON CONFLICT (process_name)
                    DO UPDATE SET extra = COALESCE({self.state_table}.extra, '{{}}'::jsonb) || EXCLUDED.extra,
                                  updated_at = now()
                    """,
                    ("__journal__", Json(new_extra))
                )
                if dropped:
                    log.warning("Auto-prune: удалено партиций: %d (retention=%d дн.)", dropped, days)
            finally:
                self.pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", ("journal_prune_global",))
        except Exception:
            log.debug("auto_prune_if_due(): skipped due to error", exc_info=True)