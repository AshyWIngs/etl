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
        compression: bool = True,       # включает сжатие транспорта; нужны lz4/zstd (cityhash обязателен)
        settings: Optional[Dict[str, Any]] = None,
        insert_chunk_size: Optional[int] = None,   # размер чанка вставки (строк)
        insert_max_retries: Optional[int] = None,  # повторов на чанк при ошибке соединения
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
            # сетевое сжатие — ZSTD даёт лучший баланс CPU/трафик
            "network_compression_method": "zstd",
            # не подменять NULL значениями по умолчанию
            "input_format_null_as_default": 0,
            # не пересчитывать TZ на сервере
            "use_client_time_zone": 1,
        }
        if settings:
            self.settings.update(settings)
        # Определяем insert_chunk_size и insert_max_retries (ленивый импорт конфига + запасные дефолты)
        cfg = None
        if insert_chunk_size is None or insert_max_retries is None:
            try:
                from scripts.config import Settings  # late import чтобы не тянуть конфиг на уровне модуля
                cfg = Settings()
            except Exception:
                cfg = None

        if insert_chunk_size is None:
            insert_chunk_size = int(getattr(cfg, "CH_INSERT_BATCH", 20000)) if cfg else 20000
        if insert_max_retries is None:
            insert_max_retries = int(getattr(cfg, "CH_INSERT_MAX_RETRIES", 1)) if cfg else 1

        self.insert_chunk_size = int(insert_chunk_size) if int(insert_chunk_size) > 0 else 20000
        self.insert_max_retries = int(insert_max_retries)
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
                    client_name="etl_codes_history",
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

    def _iter_chunks(self, rows: Iterable[Any]) -> Iterable[List[Any]]:
        """
        Порционно разбивает входной iterable на чанки по self.insert_chunk_size.
        Возвращает списки (list) для передачи драйверу.
        """
        chunk: List[Any] = []
        for row in rows:
            chunk.append(row)
            if len(chunk) >= self.insert_chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

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
        Вставка порциями (чанками). На каждую порцию — до `insert_max_retries` переподключений.
        Ожидается, что `rows` выдаёт элементы, уже приведённые к tuple/list по порядку `columns`.
        """
        table_fqn = table if "." in table else f"{self.db}.{table}"
        sql = f"INSERT INTO {table_fqn} ({', '.join(columns)}) VALUES"

        total = 0
        for chunk in self._iter_chunks(rows):
            attempt = 0
            while True:
                try:
                    self.client.execute(sql, chunk, types_check=True)
                    total += len(chunk)
                    break
                except Exception as e:
                    attempt += 1
                    if attempt > self.insert_max_retries:
                        # Пробрасываем последнюю ошибку, чтобы вызывающий код увидел фатал
                        raise
                    log.warning(
                        "Ошибка вставки в %s (chunk=%d rows): %s — переподключаюсь и повторяю (%d/%d).",
                        table_fqn, len(chunk), e, attempt, self.insert_max_retries
                    )
                    self._connect_any()
        return total

# Совместимость
CHClient = ClickHouseClient