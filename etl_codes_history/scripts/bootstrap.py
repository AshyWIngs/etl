# file: scripts/bootstrap.py
# -*- coding: utf-8 -*-
"""
Bootstrapping/инициализация окружения ETL:
- соединения с Postgres (PG) и журналом;
- соединение с Phoenix и ClickHouse;
- проверка и/или создание необходимых таблиц в ClickHouse;
- вычисление стартового окна обработки;
- общий инициализатор, который собирает все бэкенды и валидирует, что минимальные
  требования к окружению соблюдены.

Дизайн:
- Функции принимают "cfg" (любой объект с атрибутами конфигурации) и возвращают
  живые клиенты/значения.
- Исключения перехватываются там, где это повышает стабильность, и конвертируются
  в понятные сообщения (с логированием). В "горячем" пути лишнего не делаем.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, TYPE_CHECKING

log = logging.getLogger("codes_history_increment")


# -------------------------
# Мягкие протоколы (для IDE)
# -------------------------
if TYPE_CHECKING:
    class PGClientLike:
        def __init__(self, dsn: str): ...
        def close(self) -> None: ...

    class JournalLike:
        def __init__(self, pg: "PGClientLike", table: str): ...
        def mark_running(self, slice_from: datetime, slice_to: datetime, **kw: Any) -> Optional[int]: ...
        def mark_done(self, slice_from: datetime, slice_to: datetime,
                      rows_read: Optional[int] = None, rows_written: Optional[int] = None,
                      extra: Optional[Dict[str, Any]] = None) -> Optional[int]: ...
        def mark_warn(self, slice_from: datetime, slice_to: datetime, message: str,
                      component: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> Optional[int]: ...

    class PhoenixLike:
        def ping(self) -> None: ...
        def close(self) -> None: ...

    class ClickHouseLike:
        def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[List[tuple]]: ...
        def ensure_tables(self, ddl: Dict[str, str]) -> None: ...
        def ping(self) -> None: ...
else:
    PGClientLike = Any
    JournalLike = Any
    PhoenixLike = Any
    ClickHouseLike = Any


# -------------------------------------------
# Утилита: аккуратное оформление «фатальной» ошибки старта с записью в журнал
# -------------------------------------------
def startup_fail_with_journal(
    journal: Optional[JournalLike],
    *,
    slice_from: Optional[datetime] = None,
    slice_to: Optional[datetime] = None,
    message: str,
    component: str = "bootstrap",
    exc: Optional[BaseException] = None,
) -> None:
    """
    Пишем читабельное сообщение в лог и (best-effort) в журнал.
    Не выбрасываем исключение сами — решение об остановке пусть принимает вызывающий код.
    """
    # Лог — максимально информативный, но краткий
    if exc is not None:
        log.critical("FATAL: %s — %s", message, exc)
    else:
        log.critical("FATAL: %s", message)

    # Журнал — по возможности
    if journal and slice_from and slice_to:
        try:
            journal.mark_warn(slice_from, slice_to, message=message, component=component)
        except Exception:
            # Журнал сам по себе не должен ронять процесс
            pass


# -------------------------------------------
# Соединение: Postgres + журнал
# -------------------------------------------
def connect_pg_and_journal(cfg: Any) -> Tuple[PGClientLike, JournalLike]:
    """
    Создаёт PG-клиент и объект журнала.
    Требуемые атрибуты cfg:
      - PG_DSN: str
      - JOURNAL_TABLE: str (имя таблицы журнала, например "etl.journal")
    """
    # Импортируем конкретные реализации «мягко», чтобы модуль был переносимым.
    try:
        from scripts.db.pg_client import PGClient, PGConnectionError  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - под свои пути проекта
        # ЗАМЕНИ при необходимости на фактический путь к клиенту
        from scripts.db.pg_client import PGClient  # type: ignore[assignment]
        class PGConnectionError(Exception):  # type: ignore[deadCode]
            pass

    try:
        from scripts.journal import Journal  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        # ЗАМЕНИ при необходимости на фактический путь к журналу
        from scripts.journal import Journal  # type: ignore[assignment]

    dsn = getattr(cfg, "PG_DSN", "")
    if not dsn:
        raise ValueError("PG_DSN is not configured")

    journal_table = getattr(cfg, "JOURNAL_TABLE", "")
    if not journal_table:
        raise ValueError("JOURNAL_TABLE is not configured")

    try:
        pg = PGClient(dsn)
    except PGConnectionError as ex:
        # Дружественное и точное сообщение — то, что ты хотел
        msg = f"postgres connect/init failed: {ex} — возможно PostgreSQL недоступен или указан некорректный DSN"
        log.critical("FATAL: %s", msg)
        raise

    # Журнал в PG
    journal = Journal(pg, journal_table)
    return pg, journal


# -------------------------------------------
# Соединение: Phoenix
# -------------------------------------------
def connect_phx_client(cfg: Any) -> PhoenixLike:
    """
    Создаёт клиента Phoenix.
    Требуемые атрибуты cfg (минимум):
      - HBASE_ZK: str (или иные параметры, которые нужны твоей реализации клиента)
    """
    # Импорт «мягкий», поправь под свой реальный клиент при необходимости
    try:
        from scripts.phoenix_client import PhoenixClient  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        # ЗАМЕНИ путь/имя класса на фактические
        from scripts.phoenix_client import PhoenixClient  # type: ignore[assignment]

    # Собираем kwargs из cfg без жёсткой привязки — так удобнее адаптировать
    phx_kwargs: Dict[str, Any] = {}
    for name in ("HBASE_ZK", "PHOENIX_JDBC_URL", "PHOENIX_MAX_FETCH", "PHOENIX_TIMEOUT_MS"):
        if hasattr(cfg, name):
            phx_kwargs[name.lower()] = getattr(cfg, name)

    phx = PhoenixClient(**phx_kwargs)  # type: ignore[call-arg]
    # Лёгкий ранний ping — ранний fail-fast
    try:
        if hasattr(phx, "ping"):
            phx.ping()
    except Exception as ex:
        log.critical("FATAL: phoenix connect/init failed: %s", ex)
        raise
    return phx


# -------------------------------------------
# Соединение: ClickHouse
# -------------------------------------------
def connect_ch_client(cfg: Any) -> ClickHouseLike:
    """
    Создаёт клиента ClickHouse.
    Требуемые атрибуты cfg:
      - CH_DSN или набор host/port/user/password/secure/etc. в зависимости от твоего клиента.
    """
    try:
        from scripts.db.clickhouse_client import ClickHouseClient  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        # ЗАМЕНИ при необходимости на фактический путь
        from scripts.db.clickhouse_client import ClickHouseClient  # type: ignore[assignment]

    ch_kwargs: Dict[str, Any] = {}
    # Распознаём частые поля конфигов, не навязывая конкретную форму
    for name in ("CH_DSN", "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_DATABASE", "CH_SECURE"):
        if hasattr(cfg, name):
            ch_kwargs[name.lower()] = getattr(cfg, name)

    ch = ClickHouseClient(**ch_kwargs)  # type: ignore[call-arg]
    try:
        if hasattr(ch, "ping"):
            ch.ping()
    except Exception as ex:
        log.critical("FATAL: clickhouse connect/init failed: %s", ex)
        raise
    return ch


# -------------------------------------------
# Что нам нужно в ClickHouse для работы
# -------------------------------------------
@dataclass(frozen=True)
class RequiredCH:
    raw_table: str
    work_table: str
    pub_table: str


def required_ch_tables(cfg: Any) -> RequiredCH:
    """
    Возвращает имена обязательных таблиц в ClickHouse.
    Если чего-то нет в cfg — подставим разумные дефолты.
    """
    raw = getattr(cfg, "CH_RAW_TABLE", "stg_daily_codes_history_raw")
    wrk = getattr(cfg, "CH_WORK_TABLE", "stg_daily_codes_history_work")
    pub = getattr(cfg, "CH_PUB_TABLE", "stg_daily_codes_history")
    return RequiredCH(raw_table=raw, work_table=wrk, pub_table=pub)


def ensure_ch_tables_or_fail(ch: ClickHouseLike, cfg: Any) -> None:
    """
    Убеждаемся, что нужные таблицы ClickHouse существуют.
    Варианты:
      1) У клиента есть метод ensure_tables(ddl: dict[str,str]) → идеально, отдаём ему управление.
      2) Иначе — проверяем system.tables и, если что-то отсутствует, кидаем понятную ошибку.
         (DDL можно держать в отдельной папке/модуле; здесь не генерируем ради скорости/простоты).
    """
    need = required_ch_tables(cfg)

    # Если клиент умеет ensure_tables — используем (быстро и прозрачно)
    ddl_map = getattr(cfg, "CH_DDL_MAP", None)  # dict{name->sql}, если тебе удобно так хранить
    if ddl_map and hasattr(ch, "ensure_tables"):
        # Отдаём на откуп клиенту (обычно он умеет делать это эффективно и с хорошими логами)
        ch.ensure_tables(ddl_map)  # type: ignore[attr-defined]
        return

    # Лёгкая проверка через system.tables (без regex и лишних аллокаций)
    def _exists(table: str) -> bool:
        q = (
            "SELECT 1 FROM system.tables "
            "WHERE database = currentDatabase() AND name = %(t)s LIMIT 1"
        )
        try:
            res = ch.execute(q % {"t": repr(table)})  # type: ignore[operator]
            return bool(res)
        except Exception as ex:
            log.critical("FATAL: clickhouse system.tables check failed: %s", ex)
            raise

    missing: List[str] = []
    for t in (need.raw_table, need.work_table, need.pub_table):
        if not _exists(t):
            missing.append(t)

    if missing:
        # Дружелюбно, но чётко: что конкретно не хватает и где искать DDL
        hint = getattr(cfg, "CH_DDL_HINT", "Проверь папку ddl/ или модуль schema.py/publishing.py для генерации DDL.")
        raise RuntimeError(
            f"ClickHouse missing required tables: {', '.join(missing)}. "
            f"Создай их заранее. Подсказка: {hint}"
        )


# -------------------------------------------
# Подготовка стартового окна обработки
# -------------------------------------------
def bootstrap_pg_and_window(cfg: Any) -> Tuple[PGClientLike, JournalLike, datetime, datetime]:
    """
    - Подключаемся к PG и журналу.
    - Определяем стартовое окно (slice_from, slice_to) для первого цикла:
        * если заданы CFG.WINDOW_FROM / WINDOW_TO → используем их;
        * иначе — «по умолчанию»: [now - WINDOW_HOURS, now), где WINDOW_HOURS=1 если не задано.
    Примечание: Это только старт; дальше окно может смещаться итеративно.
    """
    pg, journal = connect_pg_and_journal(cfg)

    # now в UTC (наивный — для унификации с CH DateTime64 и остальным кодом)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    w_from: Optional[datetime] = getattr(cfg, "WINDOW_FROM", None)
    w_to: Optional[datetime] = getattr(cfg, "WINDOW_TO", None)
    hours = int(getattr(cfg, "WINDOW_HOURS", 1))

    if w_from is None or w_to is None:
        to_dt = now
        from_dt = to_dt - timedelta(hours=hours)
    else:
        # Если в конфиге попали aware-датеты — переведём в naive UTC
        def _naive_utc(d: datetime) -> datetime:
            if d.tzinfo is timezone.utc:
                return d.replace(tzinfo=None)
            if d.tzinfo is not None:
                return d.astimezone(timezone.utc).replace(tzinfo=None)
            return d
        from_dt = _naive_utc(w_from)
        to_dt = _naive_utc(w_to)

    return pg, journal, from_dt, to_dt


# -------------------------------------------
# Главный инициализатор бэкендов (без «горячей» логики)
# -------------------------------------------
def init_backends_and_check(cfg: Any) -> Tuple[PGClientLike, JournalLike, PhoenixLike, ClickHouseLike]:
    """
    Полный bootstrap:
      1) PG + журнал
      2) Phoenix
      3) ClickHouse + ensure required tables

    Ничего «тяжёлого» по данным не делает — только проверки доступности и структуры.
    Исключения — «дружественные» и краткие.
    """
    # 1) PG+journal
    pg: PGClientLike
    journal: JournalLike
    try:
        pg, journal = connect_pg_and_journal(cfg)
    except Exception as ex:
        msg = f"postgres connect/init failed: {ex}"
        startup_fail_with_journal(None, message=msg, exc=ex)  # журнала ещё нет
        raise

    # 2) Phoenix
    try:
        phx = connect_phx_client(cfg)
    except Exception as ex:
        msg = f"phoenix connect/init failed: {ex}"
        startup_fail_with_journal(journal, slice_from=datetime.now(timezone.utc).replace(tzinfo=None),
                                   slice_to=datetime.now(timezone.utc).replace(tzinfo=None),
                                   message=msg, exc=ex)
        raise

    # 3) ClickHouse + ensure(schema)
    try:
        ch = connect_ch_client(cfg)
        ensure_ch_tables_or_fail(ch, cfg)
    except Exception as ex:
        msg = f"clickhouse init failed: {ex}"
        startup_fail_with_journal(journal, slice_from=datetime.now(timezone.utc).replace(tzinfo=None),
                                   slice_to=datetime.now(timezone.utc).replace(tzinfo=None),
                                   message=msg, exc=ex)
        raise

    return pg, journal, phx, ch


__all__ = [
    "startup_fail_with_journal",
    "connect_pg_and_journal",
    "connect_phx_client",
    "connect_ch_client",
    "required_ch_tables",
    "ensure_ch_tables_or_fail",
    "bootstrap_pg_and_window",
    "init_backends_and_check",
]