# -*- coding: utf-8 -*-
"""
Универсальный клиент ClickHouse с авто-выбором протокола и failover.
Принимает строки и как dict, и как tuple/list — сам конвертирует под драйвер.

Зависимости:
  clickhouse-driver>=0.2.6
  clickhouse-connect==0.8.18
"""

import logging
import random
from typing import Any, Dict, Iterable, List, Optional, Sequence

log = logging.getLogger("scripts.db.clickhouse_client")


class ClickHouseClient:
    """
    Протоколы:
      - "native"  → clickhouse-driver (TCP, порт 9000)
      - "http"    → clickhouse-connect (HTTP/HTTPS, порт 8123)
    Баланс по CH_HOSTS, пинг после коннекта, авто-ретрай вставки.
    """

    def __init__(
        self,
        hosts: List[str],
        port: int = 0,
        database: Optional[str] = None,   # можно и db=...
        db: Optional[str] = None,
        user: Optional[str] = None,       # можно и username=...
        username: Optional[str] = None,
        password: Optional[str] = None,
        protocol: str = "auto",           # "auto" | "native" | "http"
        secure: bool = False,             # HTTPS (для HTTP-клиента)
        verify: bool = True,
        connect_timeout: int = 5,
        send_receive_timeout: int = 600,
        compression: bool = True,
        extra_settings: Optional[Dict[str, Any]] = None,
    ):
        self.hosts = [h.strip() for h in (hosts or []) if h and h.strip()]
        if not self.hosts:
            raise ValueError("CH_HOSTS пуст — укажи хотя бы один хост.")
        self.port = int(port or 0)

        self.db = (database or db or "default").strip() or "default"
        self.user = ((user if user is not None else username) or "default").strip() or "default"
        self.password = (password or "").strip()
        self.protocol = (protocol or "auto").lower()
        self.secure = bool(secure)
        self.verify = bool(verify)
        self.connect_timeout = int(connect_timeout)
        self.send_receive_timeout = int(send_receive_timeout)
        self.compression = bool(compression)

        # Безопасные дефолты для кластерных вставок
        self.settings: Dict[str, Any] = {
            "insert_distributed_sync": 1,
            "async_insert": 1,
            "wait_for_async_insert": 1,
        }
        if extra_settings:
            self.settings.update(extra_settings)

        self._driver: Optional[str] = None  # "native" | "http"
        self.client = None
        self.current_host: Optional[str] = None

        self._connect_auto()

    # ----------- публичный API -----------

    def insert_rows(self, table: str, rows: Iterable[Any], columns: Sequence[str]) -> int:
        """
        Вставка батча.
        Допустимые форматы rows:
          - Iterable[Dict[str, Any]]
          - Iterable[Tuple[Any, ...]] / Iterable[List[Any]]
        При ошибке соединения — один переподключение+ретрай.
        """
        table_fqn = table if "." in table else f"{self.db}.{table}"
        try:
            return self._do_insert(table_fqn, rows, columns)
        except Exception as e:
            log.warning(
                "Ошибка вставки в %s через %s: %s — пытаюсь переподключиться и повторить один раз.",
                table_fqn, self._driver or "<?>", e
            )
            self._connect_auto()
            return self._do_insert(table_fqn, rows, columns)

    def close(self):
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass

    # ----------- внутренняя кухня -----------

    def _connect_auto(self):
        hosts = self.hosts[:]
        random.shuffle(hosts)
        order = self._protocol_order()

        last_err: Optional[Exception] = None
        for proto in order:
            for host in hosts:
                try:
                    if proto == "native":
                        self._connect_native(host)
                    else:
                        self._connect_http(host)
                    self._driver = proto
                    self.current_host = host
                    log.info(
                        "Подключен к ClickHouse (%s) %s:%d, db=%s",
                        proto, host, self._effective_port(proto), self.db
                    )
                    self._ping()
                    return
                except Exception as e:
                    last_err = e
                    log.warning(
                        "Не удалось подключиться к ClickHouse на %s:%d (%s): %s",
                        host, self._effective_port(proto), proto, e
                    )
                    self.client = None
                    continue

        raise last_err or RuntimeError("Не удалось подключиться ни по native, ни по http.")

    def _protocol_order(self) -> List[str]:
        if self.protocol in ("native", "tcp"):
            return ["native"]
        if self.protocol in ("http", "https"):
            return ["http"]
        if self.port == 8123:
            return ["http", "native"]
        if self.port == 9000:
            return ["native", "http"]
        return ["native", "http"]

    def _effective_port(self, proto: str) -> int:
        return self.port or (9000 if proto == "native" else 8123)

    def _connect_native(self, host: str):
        from clickhouse_driver import Client as NativeClient
        port = self._effective_port("native")
        self.client = NativeClient(
            host=host,
            port=port,
            user=self.user,
            password=self.password,
            database=self.db,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            compression=self.compression,
            settings=self.settings,
        )

    def _connect_http(self, host: str):
        import clickhouse_connect
        port = self._effective_port("http")
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=self.user,
            password=self.password,
            database=self.db,
            secure=self.secure,
            verify=self.verify,
            connect_timeout=self.connect_timeout,
            settings=self.settings,
        )

    def _ping(self):
        if self._driver == "native":
            self.client.execute("SELECT 1")
        else:
            self.client.query("SELECT 1")

    # ---- нормализация входных строк под драйвер ----

    @staticmethod
    def _normalize_rows(
        rows: Iterable[Any],
        columns: Sequence[str],
        as_tuples: bool,
    ) -> List[Any]:
        """
        Преобразует rows к списку tuple/list длины == len(columns).
        - dict → берём значения по именам колонок
        - list/tuple → используем позиционно; подрезаем/дополняем None при несоответствии длин
        """
        col_cnt = len(columns)
        out: List[Any] = []
        for r in rows:
            if isinstance(r, dict):
                vals = [r.get(c) for c in columns]
            else:
                try:
                    seq = list(r)  # tuple/list/генератор
                except TypeError:
                    # неитерируемое единичное значение; завернём как одна колонка
                    seq = [r]
                if len(seq) < col_cnt:
                    seq += [None] * (col_cnt - len(seq))
                elif len(seq) > col_cnt:
                    seq = seq[:col_cnt]
                vals = seq
            out.append(tuple(vals) if as_tuples else list(vals))
        return out

    def _do_insert(self, table_fqn: str, rows: Iterable[Any], columns: Sequence[str]) -> int:
        if self._driver == "native":
            buf = self._normalize_rows(rows, columns, as_tuples=True)
            if not buf:
                return 0
            sql = f"INSERT INTO {table_fqn} ({', '.join(columns)}) VALUES"
            self.client.execute(sql, buf)
            return len(buf)

        # http (clickhouse-connect)
        buf = self._normalize_rows(rows, columns, as_tuples=False)
        if not buf:
            return 0
        self.client.insert(table_fqn, buf, column_names=list(columns))
        return len(buf)


# Совместимость со старым импортом
CHClient = ClickHouseClient