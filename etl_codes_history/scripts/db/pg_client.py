# file: scripts/db/pg_client.py
# -*- coding: utf-8 -*-
import logging
import psycopg
from typing import Any, Iterable, Optional, Tuple

log = logging.getLogger("scripts.db.pg_client")

class PGClient:
    """
    Очень тонкая обёртка над psycopg3:
    - контекст, курсор, базовые execute/fetch.
    - автокоммит (он практичен для простых DDL/DML в журнале).
    """

    def __init__(self, dsn: str, autocommit: bool = True):
        self.conn = psycopg.connect(dsn, autocommit=autocommit)
        self.cur = self.conn.cursor()
        log.info("PostgreSQL подключен")

    # psycopg3 уже сам адаптирует dict/list через psycopg.types.json.Json при передаче как параметра,
    # но в некоторых местах мы явно оборачиваем в journal.py для наглядности.

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> None:
        self.cur.execute(sql, params or ())

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self.cur.fetchone()

    def fetchall(self) -> list[Tuple[Any, ...]]:
        return self.cur.fetchall() or []

    def close(self):
        try:
            self.cur.close()
        finally:
            self.conn.close()