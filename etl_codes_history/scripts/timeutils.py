# file: scripts/timeutils.py
# -*- coding: utf-8 -*-
"""
Вспомогательные функции для работы со временем и таймзонами.

Сюда вынесены «горячие» и часто используемые утилиты:
- as_naive_utc_ms
- from_epoch_any_seconds_or_ms
- from_string_datetime
- to_dt64_obj
- get_tz
- iter_partitions_by_day_tz
- parse_cli_dt

Дизайн:
- Все функции «чистые» и без побочных эффектов (кроме кэша get_tz).
- Везде, где возможно, используются ранние выходы и плоские ветвления —
  это снижает когнитивную сложность и ускоряет «горячий путь».
- Возвращаемые значения согласованы с ожиданиями ClickHouse DateTime64(3):
  всегда naive UTC (для внутренних нормализаторов) или aware UTC (для CLI-парсинга).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Iterable, Iterator, Optional, Tuple
from zoneinfo import ZoneInfo


# -------------------- НОРМАЛИЗАТОРЫ ДАТ/ВРЕМЕН --------------------

def as_naive_utc_ms(dt: datetime) -> datetime:
    """
    Приводит datetime к *naive UTC* и обрезает микросекунды до миллисекунд.
    • Aware → переводим в UTC и снимаем tzinfo (phoenixdb/clickhouse ожидают naive UTC).
    • Naive → считаем, что это уже UTC, только режем до миллисекунд.
    """
    if dt.tzinfo is not None:
        # Быстрый путь: если уже UTC — избегаем astimezone
        if dt.tzinfo is timezone.utc:
            dt = dt.replace(tzinfo=None)
        else:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    if dt.microsecond:
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
    return dt


def from_epoch_any_seconds_or_ms(value: float) -> datetime:
    """
    Быстрый парсер числового таймстемпа:
    • Принимает секунды или миллисекунды (эвристика: > 10_000_000_000 → делим на 1000).
    • Возвращает naive UTC c точностью до миллисекунд.
    """
    ts = float(value)
    if ts > 10_000_000_000:
        ts = ts / 1000.0
    return as_naive_utc_ms(datetime.fromtimestamp(ts, tz=timezone.utc))


def from_string_datetime(s: str) -> Optional[datetime]:
    """
    Быстрый парсер строкового значения времени.
    Порядок:
    1) Пустая строка → None.
    2) Только цифры → epoch (sec/ms).
    3) Суффикс 'Z' → меняем на '+00:00' для fromisoformat.
    4) Пробуем ISO 8601 через datetime.fromisoformat.
    Ошибка парсинга → None (жёстких исключений не кидаем).
    """
    s = s.strip()
    if not s:
        return None
    if s.isdigit():
        return from_epoch_any_seconds_or_ms(float(s))
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return as_naive_utc_ms(datetime.fromisoformat(s))
    except Exception:
        return None


def to_dt64_obj(v: Any) -> Optional[datetime]:
    """
    Универсальный быстрый нормализатор времени для ClickHouse DateTime64(3):
    • None/"" → None
    • datetime → naive UTC (с обрезкой до миллисекунд)
    • int/float → epoch (sec или ms), naive UTC
    • str:
      – только цифры → epoch (sec/ms)
      – ISO (в т.ч. с 'Z') → fromisoformat
    Возвращает naive UTC datetime или None.
    """
    # 1) Частые пустые значения
    if v is None or v == "":
        return None

    # 2) Уже datetime
    if isinstance(v, datetime):
        return as_naive_utc_ms(v)

    # 3) Число (секунды/миллисекунды epoch)
    if isinstance(v, (int, float)):
        return from_epoch_any_seconds_or_ms(float(v))

    # 4) Прочие типы → приводим к строке один раз
    return from_string_datetime(str(v))


# -------------------- TZ и разбор CLI-дат --------------------

@lru_cache(maxsize=8)
def get_tz(tz_name: str) -> ZoneInfo:
    """
    Получение ZoneInfo по имени с небольшим кэшем.
    Не бросаемся в исключение: если зона не найдена — поднимаем стандартную ошибку,
    чтобы вызывающий код мог принять решение (или можно заменить на fallback=UTC).
    """
    return ZoneInfo(tz_name)


def iter_partitions_by_day_tz(start_dt: datetime, end_dt: datetime, tz_name: str) -> Iterator[int]:
    """
    Итератор партиций YYYYMMDD в указанной TZ для полуинтервала [start_dt; end_dt).
    • Если start_dt >= end_dt → пусто.
    • end_dt уменьшаем на 1 мс, чтобы включить день, в который попадает правая граница,
      когда она ровно на полуночь следующего дня.
    """
    if start_dt >= end_dt:
        return
    tz = get_tz(tz_name)

    def _as_tz(dt: datetime) -> datetime:
        aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        return aware.astimezone(tz)

    s_loc = _as_tz(start_dt)
    e_loc = _as_tz(end_dt - timedelta(milliseconds=1))
    d = s_loc.date()
    last = e_loc.date()
    while d <= last:
        yield d.year * 10000 + d.month * 100 + d.day
        d += timedelta(days=1)


def parse_cli_dt(value: Optional[str], mode: str, business_tz_name: str) -> Optional[datetime]:
    """
    Разбор CLI-дат:
    - mode='utc'      — наивные строки трактуем как UTC, aware → к UTC.
    - mode='business' — наивные строки трактуем в BUSINESS_TZ, затем конвертируем в UTC.
    Возвращаем *timezone-aware* UTC datetime или None.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # Пусть вызывающий код решает, как реагировать (обычно SystemExit с сообщением)
        raise SystemExit(f"Неверный формат даты/времени: {value!r}")
    if dt.tzinfo is None:
        if mode == "business":
            tz = get_tz(business_tz_name)
            dt = dt.replace(tzinfo=tz).astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


__all__ = [
    "as_naive_utc_ms",
    "from_epoch_any_seconds_or_ms",
    "from_string_datetime",
    "to_dt64_obj",
    "get_tz",
    "iter_partitions_by_day_tz",
    "parse_cli_dt",
]