# file: scripts/logging_setup.py
# -*- coding: utf-8 -*-
import logging
import sys
import re
from typing import Optional

class TruncatingFormatter(logging.Formatter):
    """
    Форматтер, который обрезает чрезмерно длинные сообщения на уровнях ≤ INFO, чтобы логи оставались читабельными.
    Обрезка применяется только если `levelno` ≤ `truncate_level`.
    """
    def __init__(
        self,
        fmt: str,
        datefmt: Optional[str] = None,
        max_message_length: Optional[int] = None,
        truncate_level: int = logging.INFO,
        ellipsis: str = "…",
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.max_message_length = max_message_length
        self.truncate_level = truncate_level
        self.ellipsis = ellipsis

    def format(self, record: logging.LogRecord) -> str:
        # Собираем исходный текст сообщения (подставляем %-аргументы).
        msg = record.getMessage()

        # Если сообщение слишком длинное и уровень не выше `truncate_level` — укорачиваем.
        if (
            self.max_message_length
            and record.levelno <= self.truncate_level
            and len(msg) > self.max_message_length
        ):
            extra = len(msg) - self.max_message_length
            msg = msg[: self.max_message_length].rstrip() + f"{self.ellipsis} (+{extra} chars)"

        # Подменяем сообщение (возможно уже укороченное), чтобы базовый Formatter использовал его.
        record.message = msg
        return super().format(record)


class PhoenixSqlShortener(logging.Filter):
    """
    Компактирует очень длинные INFO‑сообщения Phoenix SQL:
    - сворачивает перечень колонок в SELECT до `SELECT … FROM`;
    - сохраняет WHERE/params/диапазоны (самое полезное).
    Не трогает записи уровней WARNING/ERROR.
    """
    _select_re = re.compile(r"(SELECT)(.+?)(\s+FROM)", flags=re.IGNORECASE | re.DOTALL)

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno <= logging.INFO and record.name == "scripts.db.phoenix_client":
            # Пересчитываем полный текст (с учётом args) и переписываем его.
            msg = record.getMessage()
            # Сворачиваем длинный список колонок в `SELECT ... FROM`.
            msg = self._select_re.sub(r"\1 … \3", msg)
            # Сохраняем модифицированное сообщение в record для дальнейшего форматирования.
            record.msg = msg
            record.args = None
        return True

def setup_logging(
    level: str = "INFO",
    *,
    sql_logger_name: str = "scripts.db.phoenix_client",
    sql_level: str = "WARNING",
    max_message_length: Optional[int] = 300,
    datefmt: str = "%Y-%m-%d %H:%M:%S",
) -> None:
    """
    Единый формат логов для всех модулей.
    Примеры:
    2025-08-12 16:47:30 | INFO    | scripts.journal         | message...

    Дополнительно:
    - длинные сообщения на уровне INFO (например, Phoenix SQL с десятками колонок) будут
      компактированы до `SELECT … FROM ...`
    - сверхдлинные сообщения обрезаются до заданной длины с суффиксом `… (+N chars)`
    """
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    handler.setFormatter(
        TruncatingFormatter(
            fmt=fmt,
            datefmt=datefmt,                 # секунды без миллисекунд — чище в консоли
            max_message_length=max_message_length,  # ограничиваем «полотна»
            truncate_level=logging.INFO,
        )
    )
    root = logging.getLogger()
    root.setLevel(lvl)
    # Сжимаем длинные Phoenix SQL сообщения на INFO
    handler.addFilter(PhoenixSqlShortener())
    # очистим хендлеры, чтобы в ноутбуках/демонах не дублировалось
    root.handlers[:] = [handler]
    # По-умолчанию поднимаем уровень детализации для «шумных» модулей (можно вернуть DEBUG при расследованиях)
    sql_lvl = getattr(logging, (sql_level or "WARNING").upper(), logging.WARNING)
    logging.getLogger(sql_logger_name).setLevel(sql_lvl)