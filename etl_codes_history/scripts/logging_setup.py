# file: scripts/logging_setup.py
# -*- coding: utf-8 -*-
import logging
import sys

def setup_logging(level: str = "INFO") -> None:
    """
    Единый формат логов для всех модулей.
    Примеры:
    2025-08-12 16:47:30,404 | INFO    | scripts.journal | message...
    """
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.setLevel(lvl)
    # очистим хендлеры, чтобы в ноутбуках/демонах не дублировалось
    root.handlers[:] = [handler]