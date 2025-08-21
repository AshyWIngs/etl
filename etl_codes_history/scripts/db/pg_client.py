# file: scripts/db/pg_client.py
# -*- coding: utf-8 -*-
"""
PostgreSQL client (psycopg3) с управляемыми ошибками и «тихим» закрытием
-----------------------------------------------------------------------
Зачем эти изменения:
- Даём **читаемое FATAL-сообщение** при невозможности коннекта вместо «портянки»
  из внутренних трейсбеков.
- Поддерживаем **быстрый TCP-probe** перед реальным connect (опционально), чтобы
  мгновенно понять, что порт недоступен (например, сеть/файрвол) и не ждать
  долгих таймаутов HTTP / TLS / handshake.
- Ошибку коннекта заворачиваем в **PGConnectionError** — верхний уровень может
  поймать её и записать «стартовый сбой компонента postgres» в журнал.
- Финализатор сторонней библиотеки не шумит: закрытие соединения/курсора
  безопасно и без лишних логов.

Как включить быстрый отказ (опционально):
- В .env задайте `PG_TCP_PROBE_TIMEOUT_MS`, например `PG_TCP_PROBE_TIMEOUT_MS=800`.
  0 или отсутствие переменной — probe отключён (поведение как раньше).

Логи при недоступном Postgres теперь выглядят так:
  CRITICAL scripts.db.pg_client FATAL: cannot connect to PostgreSQL (host=..., port=..., db=...): OperationalError: connection timeout expired
А верхний уровень (если ловит PGConnectionError) покажет короткое сообщение
вместо полного traceback.
"""

from __future__ import annotations

import logging
import os
import socket
from typing import Any, Iterable, Optional, Tuple

import psycopg  # type: ignore
from psycopg import OperationalError  # type: ignore

try:  # удобный парсер DSN из psycopg3; если нет, упадём на urlparse
    from psycopg.conninfo import conninfo_to_dict  # type: ignore
except Exception:  # pragma: no cover - в рантайме почти всегда доступно
    conninfo_to_dict = None  # type: ignore

log = logging.getLogger("scripts.db.pg_client")

# Единая строка для критических сообщений — убираем дублирование литерала (Sonar S1192)
_LOG_FATAL = "FATAL: %s"


class PGConnectionError(Exception):
    """Выбрасывается при первичном фейле подключения к PostgreSQL."""


def _dsn_info(dsn: str) -> Tuple[str, Optional[int], Optional[str]]:
    """Достаём host, port, dbname из DSN максимально надёжно.
    Возвращаем (host or "?", port or None, dbname or None).
    """
    host: Optional[str] = None
    port: Optional[int] = None
    dbname: Optional[str] = None

    try:
        if conninfo_to_dict:  # предпочтительно: понимает как DSN, так и URI
            d = conninfo_to_dict(dsn)
            host = d.get("host") or d.get("hostaddr")
            _port = d.get("port")
            port = int(_port) if _port else None
            dbname = d.get("dbname")
        else:  # запасной путь: парсим как URI
            from urllib.parse import urlparse

            u = urlparse(dsn)
            host = u.hostname
            port = u.port
            path = u.path[1:] if u.path else ""
            dbname = path or None
    except Exception:
        # не фейлимся на парсинге: логика коннекта всё равно сработает, просто
        # в сообщении будут «?» вместо конкретики.
        pass

    return host or "?", port, dbname


def _tcp_probe(host: str, port: int, timeout_ms: int) -> None:
    """Быстрый TCP-пинг сокетом. Бросает PGConnectionError при неуспехе."""
    if timeout_ms <= 0:
        return
    try:
        with socket.create_connection((host, port), timeout_ms / 1000.0):
            return
    except Exception as e:  # соединиться не вышло — формируем управляемую ошибку
        raise PGConnectionError(
            f"TCP probe failed for {host}:{port} in {timeout_ms} ms: {e}"
        )


class PGClient:
    """
    Тонкая обёртка над psycopg3:
    - контекст, курсор, execute/fetch;
    - автокоммит по умолчанию (удобно для DDL/DML журналирования);
    - **дружественные ошибки коннекта** + аккуратное закрытие.
    """

    # Явные атрибуты соединения/курсора для статического анализа
    conn: Any | None = None
    cur: Any | None = None

    def __init__(self, dsn: str, autocommit: bool = True):
        # Атрибуты по умолчанию (важно для корректной работы статического анализатора)
        self.conn = None
        self.cur = None

        host, port, dbname = _dsn_info(dsn)

        # 1) Опциональный быстрый отказ, чтобы не ждать общий connect_timeout
        probe_ms = 0
        try:
            probe_ms = int(os.getenv("PG_TCP_PROBE_TIMEOUT_MS", "0") or "0")
        except Exception:
            probe_ms = 0
        if host != "?" and port and probe_ms > 0:
            try:
                _tcp_probe(host, port, probe_ms)
            except PGConnectionError as e:
                log.critical(_LOG_FATAL, e)
                # Даём наружу как уже «управляемую» ошибку — её словит верхний уровень
                raise

        # 2) Основной connect с ловлей OperationalError и формированием читаемого текста
        try:
            # Локальные переменные помогают статическому анализатору понять, что объекты не None
            conn = psycopg.connect(dsn, autocommit=autocommit)
            cur = conn.cursor()
            self.conn = conn
            self.cur = cur
            log.info(
                "PostgreSQL подключен (host=%s port=%s db=%s)",
                host,
                port,
                dbname or "?",
            )
        except OperationalError as e:
            msg = (
                f"cannot connect to PostgreSQL (host={host} port={port} db={dbname or '?'}): "
                f"{e.__class__.__name__}: {e}"
            )
            log.critical(_LOG_FATAL, msg)
            raise PGConnectionError(msg) from e
        except Exception as e:  # на всякий случай схлопываем все неожиданные типы
            msg = (
                f"cannot connect to PostgreSQL (host={host} port={port} db={dbname or '?'}): "
                f"{e.__class__.__name__}: {e}"
            )
            log.critical(_LOG_FATAL, msg)
            raise PGConnectionError(msg) from e

    # psycopg3 сам адаптирует dict/list через psycopg.types.json.Json при передаче,
    # но в некоторых местах мы явно оборачиваем в journal.py для наглядности.

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> None:
        """Выполнить произвольный SQL одной командой курсора.
        Поддерживается несколько выражений, разделённых `;` — PostgreSQL и psycopg3
        исполняют их последовательно в рамках одного `execute()`. Методы `fetch*`
        относятся к РЕЗУЛЬТАТУ ПОСЛЕДНЕГО `SELECT` в пакете.
        """
        c = self.cur
        if c is None:
            raise RuntimeError("Курсор PostgreSQL закрыт — соединение уже завершено")
        c.execute(sql, params or ())

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        c = self.cur
        if c is None:
            raise RuntimeError("Курсор PostgreSQL закрыт — нечего читать")
        return c.fetchone()

    def fetchall(self) -> list[Tuple[Any, ...]]:
        c = self.cur
        if c is None:
            raise RuntimeError("Курсор PostgreSQL закрыт — нечего читать")
        rows = c.fetchall()
        return rows or []

    def close(self) -> None:
        """Безопасно закрываем курсор и соединение, игнорируя ошибки в финализаторах."""
        cur = self.cur
        conn = self.conn
        try:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        finally:
            self.cur = None
            self.conn = None

    def __del__(self):  # на случай GC/аварийного пути
        try:
            self.close()
        except Exception:
            pass