# scripts/db/pg_client.py
# -*- coding: utf-8 -*-
"""
Простой клиент PostgreSQL поверх psycopg3 с удобствами для ETL:

- Автокоммит (чтобы не ловить висящие транзакции в CLI-скриптах).
- Единый курсор для простых execute/fetch операций.
- Автоматическая упаковка параметров типов dict/list в JSONB
  через psycopg.types.json.Json — это позволяет писать:
    INSERT ... VALUES (%s)  ← и передавать Python dict как параметр.
- Нормализованное логирование подключения.

Если нужно передавать сложные типы (например, datetime/date в текст),
psycopg3 обычно сам их адаптирует корректно.
"""

from __future__ import annotations
import logging
from typing import Any, Iterable, Optional, Sequence, Tuple

import psycopg
from psycopg.types.json import Json

log = logging.getLogger("scripts.db.pg_client")


class PGClient:
    """
    Минималистичная обёртка вокруг psycopg.connect.
    Использование:
        pg = PGClient(dsn)
        pg.execute("SQL ...", (param1, param2))
        row = pg.fetchone()
    """

    def __init__(self, dsn: str, autocommit: bool = True):
        """
        :param dsn: строка подключения, например:
                    'postgresql://user:pass@host:5432/dbname?options=-c%20search_path%3Dpublic'
        :param autocommit: True — включить автокоммит.
        """
        self._conn = psycopg.connect(dsn, autocommit=autocommit)
        self._cur = self._conn.cursor()
        log.info("PostgreSQL подключен")

    # ---------------------------- Внутренние утилиты ----------------------------

    @staticmethod
    def _adapt_param(value: Any) -> Any:
        """
        Автоматическая адаптация параметров для execute():
        - dict/list → Json(...) для корректной записи в JSONB.
        Иначе — возвращаем как есть (psycopg сам адаптирует базовые типы).
        """
        if isinstance(value, (dict, list)):
            return Json(value)
        return value

    @classmethod
    def _adapt_params(cls, params: Optional[Sequence[Any]]) -> Tuple[Any, ...]:
        """
        Преобразует входные параметры к tuple и адаптирует каждый элемент.
        """
        if not params:
            return tuple()
        return tuple(cls._adapt_param(p) for p in params)

    # --------------------------------- API -------------------------------------

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        """
        Выполнить произвольный SQL.
        :param sql: строка SQL с плейсхолдерами %s
        :param params: последовательность параметров (или None)
        """
        adapted = self._adapt_params(params)
        self._cur.execute(sql, adapted)

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> None:
        """
        Выполнить SQL для набора параметров.
        :param sql: строка SQL с плейсхолдерами %s
        :param seq_of_params: итерируемая коллекция параметров
        """
        def gen():
            for p in seq_of_params:
                yield self._adapt_params(p)
        self._cur.executemany(sql, gen())

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Считать одну строку результата (или None)."""
        return self._cur.fetchone()

    def fetchall(self) -> list[Tuple[Any, ...]]:
        """Считать все строки результата."""
        return self._cur.fetchall()

    def close(self) -> None:
        """Закрыть курсор и соединение."""
        try:
            self._cur.close()
        finally:
            self._conn.close()