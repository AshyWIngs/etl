# file: scripts/schema.py
# -*- coding: utf-8 -*-
"""
Единый источник правды по схеме сырых данных в ClickHouse и всем производным
константам, которые используются при дедупликации/публикации.

Зачем отдельный модуль:
- уменьшает «шум» в основном ETL, снижает когнитивную сложность;
- гарантирует, что порядок и состав колонок в INSERT/SELECT всегда согласован;
- даёт централизованную точку для будущих эволюций схемы (добавление поля и т.п.).

Важно:
- Порядок колонок CH_COLUMNS соответствует ровно входным полям источника и
  столбцам в таблицах RAW/BUF/CLEAN (кроме служебного ingested_at).
- В INSERT мы **не** передаём `ingested_at`: он заполняется в CH через
  `DEFAULT now('UTC')`. Это упрощает код и исключает несоответствие числа колонок.
"""

from __future__ import annotations

from typing import Tuple

# ---------------------------------------------------------------------------
# БАЗОВАЯ СХЕМА СЫРЫХ ДАННЫХ (RAW/BUF/CLEAN)
# ---------------------------------------------------------------------------

# Tuple вместо list — неизменяемая константа: меньше аллокаций и защита от случайной модификации.
CH_COLUMNS: Tuple[str, ...] = (
    "c", "t", "opd",
    "id", "did",
    "rid", "rinn", "rn", "sid", "sinn", "sn", "gt", "prid",
    "st", "ste", "elr",
    "emd", "apd", "exd",
    "p", "pt", "o", "pn", "b",
    "tt", "tm",
    "ch", "j",
    "pg", "et",
    "pvad", "ag",
)

# Часто используемые строковые представления (предвычисляем один раз, чтобы не делать join в «горячем пути»)
CH_COLUMNS_STR: str = ", ".join(CH_COLUMNS)

# Ключ дедупликации/публикации: по бизнес-смыслу у нас уникальность на (c, t, opd)
DEDUP_KEY_COLS: Tuple[str, ...] = ("c", "t", "opd")

# НЕКЛЮЧЕВЫЕ колонки = все колонки минус ключ (их мы агрегируем argMax(..., ingested_at))
NON_KEY_COLS: Tuple[str, ...] = tuple(c for c in CH_COLUMNS if c not in DEDUP_KEY_COLS)

# Выражение агрегации для дедупа: argMax(<col>, ingested_at) AS <col>, …
# Храним в одном месте, чтобы не разъезжались SELECT'ы в коде публикации.
DEDUP_AGG_EXPRS: str = ",\n              ".join(
    f"argMax({c}, ingested_at) AS {c}" for c in NON_KEY_COLS
)

# Полный список полей для SELECT при дедупе: ключ + агрегаты
DEDUP_SELECT_COLS: str = "c, t, opd,\n              " + DEDUP_AGG_EXPRS

# Экспортируемый интерфейс — удобнее читать и контролировать
__all__ = [
    "CH_COLUMNS",
    "CH_COLUMNS_STR",
    "DEDUP_KEY_COLS",
    "NON_KEY_COLS",
    "DEDUP_AGG_EXPRS",
    "DEDUP_SELECT_COLS",
]