# file: scripts/types.py
# -*- coding: utf-8 -*-
"""
Лёгкие Protocol-интерфейсы для клиентов и журнала.
Цели:
  • снизить плотность импортов тяжёлых реализаций в местах, где нужны только типы;
  • упростить подмену реализаций (тестовые двойники/моки);
  • сделать подсветку редактора «тише» и стабильнее.

Важно:
  • Это *структурные* типы (Protocol), а не иерархия наследования.
  • Реальные классы (PhoenixClient/ClickHouseClient/ProcessJournal) просто
    «совпадают по поверхности» — менять их код не требуется.
"""

from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    ContextManager,
    Callable,
    runtime_checkable,
)
from datetime import datetime


# -------------------- Общие алиасы для читаемости --------------------

Row = Dict[str, Any]                 # одна запись из источника
Batch = Iterable[Row]                # батч записей
Batches = Iterable[Batch]            # поток батчей


# -------------------- Phoenix (источник) --------------------

@runtime_checkable
class PhoenixLike(Protocol):
    """Минимальный интерфейс, который мы используем у Phoenix-клиента."""

    def fetch_increment_adaptive(
        self,
        table: str,
        ts_col: str,
        cols: Tuple[str, ...],
        since: datetime,   # aware UTC
        until: datetime,   # aware UTC
    ) -> Batches:
        """
        Возвращает поток батчей (итерируемых коллекций словарей).
        Каждый словарь содержит ключи, именованные по исходным колонкам.
        Границы времени — aware UTC. Внутри драйвер сам решает стратегию чтения.
        """
        ...


# -------------------- ClickHouse (приёмник) --------------------

@runtime_checkable
class CHLike(Protocol):
    """Минимальный интерфейс ClickHouse-клиента, который использует наш ETL."""

    # Быстрый потоковый INSERT «VALUES» порциями
    def insert_rows(self, table: str, rows: List[tuple], columns: Tuple[str, ...]) -> int:
        """
        Вставляет набор кортежей `rows` в таблицу `table` по схеме `columns`.
        Возвращает количество записанных строк. Может модифицировать `rows` (например, очищать).
        """
        ...

    # Универсальный SELECT/DDL/ALTER
    def execute(self, sql: str) -> Optional[List[tuple]]:
        """
        Выполняет произвольный SQL. Для SELECT возвращает список кортежей,
        для DDL/ALTER обычно возвращает None.
        """
        ...

    # Ранняя проверка наличия критичных таблиц
    def ensure_tables(
        self,
        required_tables: List[str],
        database: str,
        cluster: Optional[str],
        try_switch_host: bool = True,
    ) -> None:
        """
        Гарантирует наличие таблиц (EXISTS/CREATE). При `try_switch_host=True`
        реализация может попытаться переключиться на другой хост при сбое.
        """
        ...


# -------------------- Журнал (оркестрация) --------------------

@runtime_checkable
class JournalLike(Protocol):
    """Интерфейс, который ETL ожидает от ProcessJournal."""

    # Бутстрап и watermark
    def ensure(self) -> None: ...
    def get_watermark(self) -> Optional[datetime]: ...

    # Управление «планами» и статусами
    def clear_conflicting_planned(self, since: datetime, until: datetime) -> None: ...
    def mark_planned(self, since: datetime, until: datetime) -> None: ...
    def mark_running(self, since: datetime, until: datetime, *, host: str, pid: int) -> int: ...
    def mark_done(self, since: datetime, until: datetime, *, rows_read: int, rows_written: int) -> Optional[int]: ...
    def mark_error(self, since: datetime, until: datetime, *, message: str, component: Optional[str] = None,
                   extra: Optional[Dict[str, Any]] = None) -> Optional[int]: ...
    def mark_cancelled(self, since: datetime, until: datetime, *, message: str) -> Optional[int]: ...

    # Сердцебиение с произвольным прогрессом
    def heartbeat(self, run_id: int, *, progress: Dict[str, Any]) -> None: ...

    # Очистка «битых» запусков
    def sanitize_stale(
        self,
        *,
        planned_ttl_minutes: int,
        running_heartbeat_timeout_minutes: int,
        running_hard_ttl_hours: int,
    ) -> None: ...

    # Advisory-lock как контекстный менеджер: `with journal.exclusive_lock() as got:`
    def exclusive_lock(self) -> ContextManager[bool]: ...

    # Стартап-диагностика (best-effort, может отсутствовать — поэтому в коде обычно через getattr)
    def mark_startup_error(
        self,
        message: str,
        *,
        component: str,
        since: datetime,
        until: datetime,
        host: str,
        pid: int,
        extra: Dict[str, Any],
    ) -> None: ...