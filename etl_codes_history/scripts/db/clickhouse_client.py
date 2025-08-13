# file: scripts/db/clickhouse_client.py
# -*- coding: utf-8 -*-
"""
Только Native (TCP, порт 9000), с компрессией.
Список хостов — failover с проверкой SELECT 1.
Вставка батчами, auto retry 1 раз при отвале коннекта.
"""
import logging
import random
from typing import Any, Dict, Iterable, List, Optional, Sequence

from clickhouse_driver import Client as NativeClient

log = logging.getLogger("scripts.db.clickhouse_client")

class ClickHouseClient:
    def __init__(
        self,
        hosts: List[str],
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
        connect_timeout: int = 5,
        send_receive_timeout: int = 600,
        compression: bool = True,       # включено: требует clickhouse-cityhash + lz4/zstd
        settings: Optional[Dict[str, Any]] = None,
    ):
        self.hosts = [h.strip() for h in (hosts or []) if h and h.strip()]
        if not self.hosts:
            raise ValueError("CH_HOSTS пуст — укажи хотя бы один хост.")
        self.port = int(port)
        self.db = database
        self.user = user
        self.password = password
        self.connect_timeout = int(connect_timeout)
        self.send_receive_timeout = int(send_receive_timeout)
        self.compression = bool(compression)

        self.settings: Dict[str, Any] = {
            "async_insert": 1,
            "wait_for_async_insert": 1,
            "insert_distributed_sync": 1,
        }
        if settings:
            self.settings.update(settings)

        self.client: Optional[NativeClient] = None
        self.current_host: Optional[str] = None
        self._connect_any()

    def _connect_any(self) -> None:
        last_err: Optional[Exception] = None
        hosts = self.hosts[:]
        random.shuffle(hosts)
        for host in hosts:
            try:
                self.client = NativeClient(
                    host=host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.db,
                    connect_timeout=self.connect_timeout,
                    send_receive_timeout=self.send_receive_timeout,
                    compression=self.compression,
                    settings=self.settings,
                )
                self.client.execute("SELECT 1")
                self.current_host = host
                log.info("Подключен к ClickHouse (native) %s:%d, db=%s", host, self.port, self.db)
                return
            except Exception as e:
                last_err = e
                log.warning("Не удалось подключиться к ClickHouse на %s:%d: %s", host, self.port, e)
                self.client = None
                continue
        raise last_err or RuntimeError("Не удалось подключиться к ClickHouse ни к одному хосту.")

    def close(self):
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass

    def query_scalar(self, sql: str) -> Any:
        res = self.client.execute(sql)
        return res[0][0] if res else None

    def insert_rows(self, table: str, rows: Iterable[Any], columns: Sequence[str]) -> int:
        """
        Вставка батча (rows уже приведены к tuple/list по порядку columns).
        На ошибках соединения пытаемся переподключиться и повторить один раз.
        """
        table_fqn = table if "." in table else f"{self.db}.{table}"
        buf = list(rows)
        if not buf:
            return 0
        sql = f"INSERT INTO {table_fqn} ({', '.join(columns)}) VALUES"
        try:
            self.client.execute(sql, buf)
            return len(buf)
        except Exception as e:
            log.warning("Ошибка вставки в %s: %s — переподключаюсь и повторяю.", table_fqn, e)
            self._connect_any()
            self.client.execute(sql, buf)
            return len(buf)

# Совместимость
CHClient = ClickHouseClient