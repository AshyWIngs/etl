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
import re
from clickhouse_driver import errors as ch_errors

log = logging.getLogger("scripts.db.clickhouse_client")

# Регулярка для безопасной проверки «это INSERT?» с учётом пробелов и комментариев
_INSERT_RE = re.compile(
    r"^\s*(?:--.*?$|\s|/\*.*?\*/)*INSERT\b",
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

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

    def _is_insert(self, sql: str) -> bool:
        """
        Возвращает True, если sql начинается с INSERT (игнорируя пробелы и комментарии).
        """
        return bool(_INSERT_RE.match(sql or ""))

    def _clear_force_insert(self) -> None:
        """
        Страховка от бага clickhouse-driver: после INSERT соединение может
        остаться в состоянии «force insert». Для любых не-INSERT запросов
        обнуляем внутренние флаги соединения, если они присутствуют.
        """
        try:
            conn = getattr(self.client, "connection", None)
            if conn is not None:
                if hasattr(conn, "force_insert_query"):
                    conn.force_insert_query = False
                if hasattr(conn, "force_insert"):
                    conn.force_insert = False
        except Exception:
            # best-effort: не мешаем основному потоку
            pass

    def _execute_once(
        self,
        sql: str,
        params: Optional[Sequence] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        Единичный вызов self.client.execute с безопасной обработкой не-INSERT запросов.
        - для не-INSERT: сбрасываем force_insert* флаги и не передаём пустой params вовсе.
        - для INSERT: ведём себя стандартно.
        """
        is_insert = self._is_insert(sql)
        exec_settings = settings or {}
        if not is_insert:
            self._clear_force_insert()
            # ВАЖНО: если params пустой — НЕ передавать его, чтобы драйвер не переключился на insert-ветку
            if params is None or (isinstance(params, (list, tuple)) and len(params) == 0):
                return self.client.execute(sql, settings=exec_settings)
            else:
                return self.client.execute(sql, params=params, settings=exec_settings)
        else:
            if params is None or (isinstance(params, (list, tuple)) and len(params) == 0):
                return self.client.execute(sql, settings=exec_settings)
            else:
                return self.client.execute(sql, params=params, settings=exec_settings)

    def _execute_with_retry(
        self,
        sql: str,
        params: Optional[Sequence] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        Один автоматический повтор при UnexpectedPacketFromServerError:
        - на повторе выполняем reconnect() и делаем второй вызов.
        Это покрывает редкий флап после bulk INSERT/разрывов соединения,
        когда драйвер ошибочно ждёт пакеты «как при вставке».
        """
        try:
            return self._execute_once(sql, params=params, settings=settings)
        except ch_errors.UnexpectedPacketFromServerError as e:
            # типичный флап: сервер закрыл соединение, а клиент ждал "sample block/Data"
            log.warning(
                "UnexpectedPacketFromServerError на '%s': %s — reconnect() и один повтор.",
                (sql.split()[0] if sql else "query"), e
            )
            self.reconnect()
            return self._execute_once(sql, params=params, settings=settings)

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
        """
        Выполняет SELECT и возвращает первое скалярное значение.
        Использует безопасный путь исполнения + 1 ретрай при UnexpectedPacketFromServerError.
        """
        res = self._execute_with_retry(sql)
        return res[0][0] if res else None

    def execute(self, sql: str, params: Optional[Sequence] = None, settings: Optional[Dict[str, Any]] = None) -> None:
        """
        Универсальный вызов (DDL/DML/ALTER/INSERT SELECT) с поддержкой settings.
        Защита от «залипания» драйвера в insert-режиме и 1 авто-повтор при
        UnexpectedPacketFromServerError.
        """
        self._execute_with_retry(sql, params=params, settings=settings)

    def query_all(self, sql: str, params: Optional[Sequence] = None, settings: Optional[Dict[str, Any]] = None) -> List[tuple]:
        """
        SELECT → список строк. Использует безопасный путь исполнения
        (сброс force_insert для не-INSERT) и 1 авто-повтор при UnexpectedPacketFromServerError.
        """
        return self._execute_with_retry(sql, params=params, settings=settings)  # type: ignore[return-value]

    def reconnect(self) -> None:
        """
        Принудительное переподключение (обход глюков драйвера после bulk INSERT).
        """
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass
        self._connect_any()

    def insert_rows(self, table: str, rows: Iterable[Any], columns: Sequence[str]) -> int:
        """
        Вставка порциями (чанками). На каждую порцию — до `insert_max_retries` переподключений.
        Ожидается, что `rows` выдаёт элементы, уже приведённые к tuple/list по порядку `columns`.
        Здесь НЕ трогаем никаких внутренних "force insert" флагов — драйвер корректно
        определяет режим по самому SQL (INSERT), а для не-INSERT это делается в _execute_*().
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
