# file: scripts/db/phoenix_client.py
# -*- coding: utf-8 -*-
"""
PhoenixClient — лёгкая обёртка над `phoenixdb` для чтения инкремента из Apache Phoenix PQS.

Возможности и поведение:
- Подключение с авто-выбором протокола: сперва пробуем Avatica protobuf, если библиотека его не поддерживает
  (TypeError) или подключение падает — корректно откатываемся на JSON. Если оба способа не удались, поднимаем
  `PhoenixConnectionError` и пишем компактный CRITICAL-лог вида `FATAL: cannot connect to Phoenix PQS ...`.
- Быстрый fail-fast перед коннектом: опциональный TCP-probe до `host:port` (см. `PHOENIX_TCP_PROBE_TIMEOUT_MS`),
  чтобы не ждать долгих таймаутов внутри `requests`/`urllib3`.
- Подавление «портянок» из финализатора `phoenixdb.Connection.__del__`: при GC недоступного PQS библиотека
  иногда печатает огромные stacktrace. Мы безопасно перехватываем исключения внутри `__del__`, чтобы логи были
  компактными.
- Настроенный размер выборки: `cursor.arraysize = fetchmany_size`, чтобы `fetchmany()` реально возвращал кадры
  нужного масштаба.
- Прозрачные логи: при каждом запросе логируем SQL, окно `[from; to)`, `ts_units` и размер пакета.
- Утилиты:
    * `discover_columns(table)` — быстрый способ получить список колонок по `SELECT * LIMIT 0`;
    * `fetch_increment(table, ts_col, columns, from_dt, to_dt)` — итератор по блокам словарей, упорядочено по `ts_col`.

Контракты:
- `from_dt`/`to_dt` должны быть timezone-aware (UTC). Драйвер корректно сериализует UTC в Phoenix.
- `paramstyle=qmark` — плейсхолдеры `?`.
- Закрытие: `close()` всегда пытается закрыть курсор, затем соединение; все ошибки при закрытии гасим.

Исключения:
- `PhoenixConnectionError` — не удалось установить соединение ни через protobuf, ни через JSON. Это сигнал для
  внешнего уровня (ETL-скрипта) записать «стартовую» ошибку в журнал и завершиться с ненулевым кодом.

Переменные окружения:
- `PHOENIX_TCP_PROBE_TIMEOUT_MS` — необязательный таймаут (в мс) TCP-проверки доступности PQS перед коннектом.
  0 или отсутствие переменной — проверка отключена.
"""
import logging
from typing import Dict, Iterator, List, Any, Optional
from datetime import datetime

# Динамический импорт phoenixdb: снимаем предупреждение Pylance в IDE,
# при настоящем запуске пакет всё равно должен быть установлен.
try:
    import importlib
    phoenixdb = importlib.import_module("phoenixdb")  # type: ignore[import-not-found]
except Exception:
    phoenixdb = None  # type: ignore[assignment]

import os
import socket
from urllib.parse import urlparse

# --- Noise suppression for phoenixdb.Connection.__del__ -------------------
# Некоторые версии `phoenixdb` в __del__ выполняют сетевые вызовы и могут печатать
# "Exception ignored in: ... Connection.__del__" при недоступном PQS. Чтобы не плодить
# портянки в логах, аккуратно перехватываем исключения в финализаторе.
try:
    import importlib
    _phx_conn_mod = importlib.import_module("phoenixdb.connection")  # type: ignore[import-not-found]
    _orig_del = getattr(_phx_conn_mod.Connection, "__del__", None)
    if callable(_orig_del):
        def _quiet_del(self):  # type: ignore
            try:
                _orig_del(self)  # type: ignore
            except Exception:
                # Молча игнорируем ошибки при сборке мусора (PQS недоступен и т.п.)
                return
        _phx_conn_mod.Connection.__del__ = _quiet_del  # type: ignore
except Exception:
    # Если в будущих версиях API изменится — ничего страшного, оставим поведение по умолчанию.
    pass

# --- Fast-fail TCP probe ---------------------------------------------------
def _tcp_probe(pqs_url: str, timeout_ms: int) -> None:
    """
    Быстрая проверка доступности TCP-сокета Phoenix PQS перед созданием phoenixdb.Connection.
    Поднимает PhoenixConnectionError при недоступности.
    """
    if timeout_ms <= 0:
        return
    u = urlparse(pqs_url)
    host = u.hostname or "127.0.0.1"
    port = u.port or 8765
    try:
        with socket.create_connection((host, port), timeout=timeout_ms / 1000.0):
            return
    except Exception as e:
        raise PhoenixConnectionError(f"TCP probe failed for {host}:{port} in {timeout_ms} ms: {e}")

log = logging.getLogger("scripts.db.phoenix_client")

class PhoenixConnectionError(Exception):
    """Ошибка подключения к Phoenix PQS (оба способа подключения провалились)."""

class PhoenixClient:
    """
    Подключается к Phoenix PQS и позволяет:
    - получить список колонок (LIMIT 0),
    - постранично читать инкремент за интервал [from; to).
    """

    conn: Optional[Any] = None
    cur: Optional[Any] = None

    def __init__(self, pqs_url: str, fetchmany_size: int = 5000, ts_units: str = "timestamp"):
        """
        Инициализация клиента Phoenix PQS.

        Args:
            pqs_url: База Avatica (например, http://host:8765).
            fetchmany_size: Размер кадра для fetchmany().
            ts_units: Информационный атрибут для логов ('timestamp' | 'millis' | 'seconds').

        Raises:
            PhoenixConnectionError: если не удалось подключиться ни по protobuf, ни по JSON.
        """
        # Fail fast: при включённом PHOENIX_TCP_PROBE_TIMEOUT_MS быстро проверяем доступность host:port
        try:
            probe_ms = int(os.getenv("PHOENIX_TCP_PROBE_TIMEOUT_MS", "0") or "0")
        except Exception:
            probe_ms = 0
        if probe_ms > 0:
            try:
                _tcp_probe(pqs_url, probe_ms)
            except PhoenixConnectionError as probe_err:
                log.critical("FATAL: %s", probe_err)
                raise
            except Exception as probe_err:
                # Приводим к нашему типу исключения
                log.critical("FATAL: %s", probe_err)
                raise PhoenixConnectionError(str(probe_err))

        if phoenixdb is None:
            msg = "Библиотека 'phoenixdb' не установлена в активном окружении"
            log.critical("FATAL: %s", msg)
            raise PhoenixConnectionError(msg)

        serialization_used = "protobuf"
        try:
            # Попытка №1: явный protobuf (на новых версиях phoenixdb быстрее JSON)
            try:
                conn = phoenixdb.connect(
                    pqs_url,
                    autocommit=True,
                    serialization="protobuf",
                )
            except TypeError:
                # Библиотека не знает параметр `serialization` — пробуем дефолт (JSON)
                serialization_used = "json"
                conn = phoenixdb.connect(
                    pqs_url,
                    autocommit=True,
                )
            except Exception as e:
                # Протокол protobuf «доступен», но подключение не удалось — откат на JSON
                log.warning("Phoenix protobuf connect failed (%s) — falling back to JSON.", e)
                serialization_used = "json"
                conn = phoenixdb.connect(
                    pqs_url,
                    autocommit=True,
                )
        except Exception as json_err:
            # И JSON-подключение не удалось — поднимаем аккуратное исключение с коротким сообщением
            msg = f"cannot connect to Phoenix PQS {pqs_url}: {json_err}"
            log.critical("FATAL: %s", msg)
            raise PhoenixConnectionError(msg) from json_err

        # Присваиваем только после успешного коннекта
        self.conn = conn

        # Локальная ссылка + assert для узкого типа: подсказка анализатору, что объект не None
        conn_local = self.conn
        assert conn_local is not None
        self.cur = conn_local.cursor()
        self.fetchmany_size = int(fetchmany_size)
        self.ts_units = ts_units
        # Подсказка драйверу о размере кадров, строго по отступам
        cur_local = self.cur
        if cur_local is not None:
            cur_local.arraysize = self.fetchmany_size
        self._serialization = serialization_used
        log.info("Phoenix PQS подключен (%s), paramstyle=qmark, serialization=%s", pqs_url, serialization_used)

    def close(self):
        """
        Закрываем курсор и соединение, не шумим, если PQS уже недоступен.
        """
        try:
            cur = getattr(self, "cur", None)
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            try:
                conn = getattr(self, "conn", None)
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            finally:
                # Чётко обнуляем ссылки — меньше шансов словить шум в __del__
                self.cur = None
                self.conn = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _cur(self):
        """
        Возвращает активный курсор; если клиент закрыт — кидает RuntimeError.
        Нужен для того, чтобы статический анализ понимал, что дальше объект не None.
        """
        cur = getattr(self, "cur", None)
        if cur is None:
            raise RuntimeError("Клиент Phoenix закрыт: курсор недоступен")
        return cur

    def discover_columns(self, table: str) -> List[str]:
        """
        SELECT * LIMIT 0 → имена колонок как в описании таблицы.
        """
        cur = self._cur()
        cur.execute(f'SELECT * FROM "{table}" LIMIT 0')
        return [d[0] for d in (cur.description or [])]

    def fetch_increment(
        self,
        table: str,
        ts_col: str,
        columns: List[str],
        from_dt: datetime,
        to_dt: datetime,
    ) -> Iterator[List[Dict]]:
        """
        Читает блоками (fetchmany_size) интервал [from_dt, to_dt), упорядочено по ts_col.
        Возвращает список словарей {col: value}.
        from_dt/to_dt должны быть aware (UTC) — phoenixdb их корректно сериализует.
        """
        cols_quoted = ", ".join(f'"{c}"' for c in columns)
        sql = (
            f'SELECT {cols_quoted} FROM "{table}" '
            f'WHERE "{ts_col}" >= ? AND "{ts_col}" < ? '
            f'ORDER BY "{ts_col}"'
        )
        log.info(
            "Phoenix SQL: %s | params: [%s → %s] | ts_units=%s | fetchmany=%d",
            sql.replace("\n", " "),
            from_dt.isoformat(), to_dt.isoformat(),
            self.ts_units, self.fetchmany_size
        )
        cur = self._cur()
        cur.execute(sql, (from_dt, to_dt))

        # Имена колонок в рамках одного запроса неизменны — вычисляем один раз.
        # Некоторые версии драйвера могут отложенно заполнять description,
        # поэтому допускаем отложенное вычисление при первой выборке.
        names: Optional[List[str]] = [d[0] for d in (cur.description or [])] or None

        # Локальные ссылки на builtins/переменные — дешёвая микрооптимизация,
        # убирает глобальные поиска имён внутри горячего цикла.
        dict_ = dict
        zip_ = zip
        size = self.fetchmany_size

        while True:
            rows = cur.fetchmany(size)
            if not rows:
                break
            if names is None:
                names = [d[0] for d in (cur.description or [])]
            n = names  # локальная ссылка для list comprehension ниже
            out = [dict_(zip_(n, r)) for r in rows]
            yield out