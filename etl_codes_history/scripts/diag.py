# file: scripts/diag.py
# -*- coding: utf-8 -*-
"""
Диагностика/подсказки: трейсбек-политика и короткие сообщения о выбранном окне запуска.

Здесь собраны функции, которые раньше жили в codes_history_etl.py:
- want_trace        — решает, когда печатать traceback
- log_maybe_trace   — логирование ошибки с/без traceback (+подсказка пользователю)
- log_window_hints  — компактные подсказки про since/until
"""

from __future__ import annotations

import logging
import os
from typing import Optional, Any, TYPE_CHECKING

# Чтобы не создавать циклических импортов в рантайме, тип Settings импортируем
# только для анализатора типов.
if TYPE_CHECKING:  # pragma: no cover
    from .config import Settings  # noqa: F401

log = logging.getLogger("codes_history_increment")


def want_trace(cfg: Optional["Settings"] = None) -> bool:
    """
    Возвращает True, если нужно печатать traceback.

    Правила:
    • Если в конфиге ETL_TRACE_EXC=1 — печатаем.
    • Если уровень логгера DEBUG — печатаем.
    • Если cfg ещё не создан, смотрим переменную окружения ETL_TRACE_EXC.
    """
    try:
        if cfg is not None and bool(getattr(cfg, "ETL_TRACE_EXC", False)):
            return True
    except Exception:
        # Конфиг может быть с частично определёнными атрибутами
        pass

    env_flag = os.getenv("ETL_TRACE_EXC", "0").lower() in ("1", "true", "yes", "on")
    # Если установлен DEBUG у нашего логгера — тоже выводим стек
    return env_flag or logging.getLogger("codes_history_increment").isEnabledFor(logging.DEBUG)


def log_maybe_trace(level: int, msg: str, *, exc: Optional[BaseException] = None, cfg: Optional["Settings"] = None) -> None:
    """
    Единый помощник для логирования ошибок: с трейсбеком или без.
    Если стек скрыт, добавляет короткую подсказку как его включить.
    """
    if exc is not None and want_trace(cfg):
        log.log(level, msg, exc_info=True)
        return

    if exc is not None:
        msg = (
            msg
            + " (стек скрыт; установите ETL_TRACE_EXC=1 или запустите с --log-level=DEBUG, чтобы напечатать трейсбек)"
        )
    log.log(level, msg)


def log_window_hints(used_watermark: bool, auto_until: bool, auto_window_hours: int) -> None:
    """
    Компактные подсказки по окну запуска.
    Стараемся не бросать исключения — логи «best effort».
    """
    try:
        if used_watermark:
            log.info("since не указан: использую watermark из журнала (inc_process_state).")
        if auto_until:
            log.info(
                "until не указан: использую since + %d ч (RUN_WINDOW_HOURS).",
                int(auto_window_hours),
            )
    except Exception:
        # Логирование не должно мешать основной работе
        pass

# Экспортируемый интерфейс — удобнее читать и контролировать
__all__ = [
    "want_trace",
    "log_maybe_trace",
    "log_window_hints",
]