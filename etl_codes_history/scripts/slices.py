# file: scripts/slices.py
# -*- coding: utf-8 -*-
"""
Слайсинг и «горячая» обработка данных из Phoenix в ClickHouse.

Отвечает за:
- конвертацию рядов Phoenix -> кортежи ClickHouse в нужном порядке колонок;
- буферизацию и быструю вставку батчами в ClickHouse;
- обработку одного слайса окна (fetch -> process -> flush) с журналированием;
- вторичные утилиты (разбор хранилища CH при необходимости, вычисление партиции по UTC).

ВАЖНО:
- Здесь нет логики публикации/дедупа (это в publishing.py).
- Здесь нет коннектов и ensure-схем (это в bootstrap.py).
- Чтобы не тормозить «горячий» путь, все тяжёлые обращения, проверки и regex’ы сведены к нулю.

Совместимость:
- Имена функций совпадают с теми, что раньше были в монолите: _process_one_slice,
  _proc_phx_batch, _flush_ch_buffer, _row_to_ch_tuple, _opd_to_part_utc, _parse_ch_storage.
- Добавлен адаптер process_one_slice(...) под новый вызов из оркестратора.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, TYPE_CHECKING

log = logging.getLogger("codes_history_increment")

# ---- Импорт схемы колонок CH (порядок критичен для быстрой вставки) ----
try:
    from .schema import CH_COLUMNS, CH_DT_FIELDS, CH_INT_FIELDS  # type: ignore[attr-defined]
except Exception:
    # Fallback: минимально достаточный набор. Как только добавишь schema.py — этот блок можно удалить.
    CH_COLUMNS: Tuple[str, ...] = (
        "c","t","opd",                    # составной ключ (пример)
        "id","did",
        "rid","rinn","rn","sid","sinn","sn","gt","prid",
        "st","ste","elr",
        "emd","apd","exd",               # даты/времена доменные
        "p","pt","o","pn","b",
        "tt","tm",                       # метки времени
        "ch","j",
        "pg","et",
        "pvad","ag",
        # ingested_at не перечисляем — он вычисляется/добавляется на другой стадии (RAW)
    )
    CH_DT_FIELDS: Set[str] = {"opd", "emd", "apd", "exd", "tt", "tm"}
    CH_INT_FIELDS: Set[str] = {"id", "did", "rid", "rinn", "rn", "sid", "sinn", "st", "ste", "elr", "pt", "o", "b", "pg", "et", "pvad", "ag"}

# ---- Типы клиентов (мягкие протоколы) ----
if TYPE_CHECKING:
    class PhoenixLike:
        def fetch_increment_adaptive(
            self,
            table: str, ts_col: str, columns: Sequence[str],
            from_dt: datetime, to_dt: datetime,
        ) -> Iterator[List[Dict[str, Any]]]: ...

    class ClickHouseLike:
        def execute(self, sql: str, *params: Any) -> Optional[List[tuple]]: ...
        def insert_values(self, table: str, columns: Sequence[str], rows: Sequence[tuple]) -> int: ...

    class JournalLike:
        def mark_running(self, slice_from: datetime, slice_to: datetime, **kw: Any) -> Optional[int]: ...
        def mark_done(self, slice_from: datetime, slice_to: datetime, rows_read: Optional[int] = None,
                      rows_written: Optional[int] = None, extra: Optional[Dict[str, Any]] = None) -> Optional[int]: ...
        def mark_warn(self, slice_from: datetime, slice_to: datetime, message: str,
                      component: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> Optional[int]: ...
else:
    PhoenixLike = Any
    ClickHouseLike = Any
    JournalLike = Any


# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ УТИЛИТЫ (микрооптимизированы под горячий путь)
# -----------------------------------------------------------------------------

def _to_utc_naive(dt: Any) -> Optional[datetime]:
    """Гарантируем naive UTC (tzinfo=None) для разных входов (datetime/epoch/ISO-строка)."""
    if dt is None:
        return None

    if isinstance(dt, datetime):
        if dt.tzinfo is timezone.utc:
            return dt.replace(tzinfo=None)
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    if isinstance(dt, (int, float)):
        if dt > 10**12:  # эвристика: мс
            dt = dt / 1000.0
        return datetime.utcfromtimestamp(float(dt))

    if isinstance(dt, str):
        s = dt.strip()
        if not s:
            return None
        s = s.replace("T", " ")
        if s.endswith("Z"):
            s = s[:-1]
        main, dot, frac = s.partition(".")
        try:
            base = datetime.strptime(main, "%Y-%m-%d %H:%M:%S")
            if dot and frac:
                us = int((frac + "000000")[:6])
                base = base.replace(microsecond=us)
            return base
        except Exception:
            return None

    return None


def _opd_to_part_utc(opd: Any) -> Optional[int]:
    """Перевод opc/opd в номер партиции UTC (YYYYMMDD)."""
    dt = _to_utc_naive(opd)
    if dt is None:
        return None
    return dt.year * 10000 + dt.month * 100 + dt.day


def _row_to_ch_tuple(row: Dict[str, Any],
                     columns: Sequence[str] = CH_COLUMNS,
                     dt_fields: Set[str] = CH_DT_FIELDS,
                     int_fields: Set[str] = CH_INT_FIELDS) -> tuple:
    """Быстрая конвертация dict -> tuple под порядок колонок ClickHouse."""
    get = row.get
    out: List[Any] = [None] * len(columns)
    for i, col in enumerate(columns):
        v = get(col)
        if v is None:
            out[i] = None
            continue
        if col in dt_fields:
            out[i] = _to_utc_naive(v)
        elif col in int_fields:
            try:
                out[i] = int(v)
            except Exception:
                out[i] = None
        else:
            out[i] = v
    return tuple(out)


# -----------------------------------------------------------------------------
# БЫСТРАЯ ВСТАВКА В CH
# -----------------------------------------------------------------------------

def _flush_ch_buffer(
    ch: ClickHouseLike,
    table: str,
    buf: List[tuple],
    columns: Sequence[str] = CH_COLUMNS,
    insert_values: Optional[Callable[[str, Sequence[str], Sequence[tuple]], int]] = None,
) -> int:
    """Слив батча в CH. Возвращает число записанных строк. Ничего не делает, если buf пуст."""
    if not buf:
        return 0

    n = len(buf)
    execute: Callable[[str], Optional[List[tuple]]] = ch.execute

    if insert_values is None:
        insert_values = getattr(ch, "insert_values", None)

    if callable(insert_values):
        try:
            return int(insert_values(table, columns, buf))
        except Exception as ex:
            log.debug("insert_values не доступен/упал (%s), fallback на INSERT VALUES", ex)

    cols = ", ".join(columns)
    values_sql = f"INSERT INTO {table} ({cols}) VALUES"
    placeholders = ",".join(["(" + ",".join(["%s"] * len(columns)) + ")"] * n)

    flat_params: List[Any] = []
    ext = flat_params.extend
    for t in buf:
        ext(t)

    try:
        execute(values_sql + placeholders, flat_params)
        return n
    except TypeError:
        written = 0
        for t in buf:
            single_ph = "(" + ",".join(["%r"] * len(columns)) + ")"
            sql = values_sql + single_ph % t
            execute(sql)
            written += 1
        return written


# -----------------------------------------------------------------------------
# ОБРАБОТКА БАТЧЕЙ И СЛАЙСОВ
# -----------------------------------------------------------------------------

def _proc_phx_batch(
    rows: Sequence[Dict[str, Any]],
    out_buf: List[tuple],
    to_tuple: Callable[[Dict[str, Any]], tuple] = _row_to_ch_tuple,
) -> int:
    """Проход по батчу из Phoenix с конвертацией в кортежи CH (без флаша)."""
    if not rows:
        return 0
    append = out_buf.append
    for r in rows:
        append(to_tuple(r))
    return len(rows)


def _parse_ch_storage(ch: ClickHouseLike, table: str) -> Dict[str, Any]:
    """Чтение базовой информации о таблице CH из system.tables (engine, sorting_key, partition_key)."""
    info: Dict[str, Any] = {"engine": None, "sorting_key": None, "partition_key": None}
    try:
        q = (
            "SELECT engine, sorting_key, partition_key "
            "FROM system.tables "
            "WHERE database = currentDatabase() AND name = %(t)s"
        )
        res = ch.execute(q % {"t": repr(table)})
        if res and len(res) > 0:
            eng, sk, pk = res[0]
            info["engine"] = eng
            info["sorting_key"] = sk
            info["partition_key"] = pk
    except Exception as ex:
        log.debug("parse_ch_storage: системный запрос не удался (%s); вернём пустую информацию", ex)
    return info


def _process_one_slice(
    phx: PhoenixLike,
    ch: ClickHouseLike,
    journal: JournalLike,
    *,
    table: str,
    ts_col: str,
    columns: Sequence[str],
    slice_from: datetime,
    slice_to: datetime,
    ch_target_table: str,
    ch_batch_rows: int = 10_000,
    to_tuple: Callable[[Dict[str, Any]], tuple] = _row_to_ch_tuple,
    insert_values: Optional[Callable[[str, Sequence[str], Sequence[tuple]], int]] = None,
    add_part_when_empty: bool = True,
) -> Tuple[int, int, Set[int]]:
    """
    Полный «старый» обработчик слайса: Phoenix → кортежи → батчи → INSERT в CH.
    Возвращает (rows_read, rows_written, parts_utc).
    """
    fetch_iter = phx.fetch_increment_adaptive
    _flush = _flush_ch_buffer
    execute = ch.execute
    append_part = set.add

    try:
        journal.mark_running(slice_from, slice_to)
    except Exception:
        pass

    rows_read = 0
    rows_written = 0
    parts: Set[int] = set()
    buffer: List[tuple] = []

    try:
        for batch in fetch_iter(table, ts_col, columns, slice_from, slice_to):
            if not batch:
                continue

            rows_read += _proc_phx_batch(batch, buffer, to_tuple)

            for r in batch:
                p = _opd_to_part_utc(r.get("opd"))
                if p is not None:
                    append_part(parts, p)

            if len(buffer) >= ch_batch_rows:
                rows_written += _flush(ch, ch_target_table, buffer, CH_COLUMNS, insert_values)
                buffer.clear()

        if buffer:
            rows_written += _flush(ch, ch_target_table, buffer, CH_COLUMNS, insert_values)
            buffer.clear()

        if not add_part_when_empty and rows_read == 0:
            parts.clear()

    except Exception as ex:
        try:
            journal.mark_warn(slice_from, slice_to, f"slice processing failed: {ex}", component="slices")
        except Exception:
            pass
        raise
    finally:
        try:
            journal.mark_done(slice_from, slice_to, rows_read=rows_read, rows_written=rows_written,
                              extra={"parts": sorted(parts) if parts else []})
        except Exception:
            pass

    return rows_read, rows_written, parts


# -----------------------------------------------------------------------------
# НОВЫЕ ХЕЛПЕРЫ/АДАПТЕРЫ ДЛЯ ОРКЕСТРАТОРА
# -----------------------------------------------------------------------------

def resolve_phx_table_and_cols(cfg: Any) -> Tuple[str, str, Tuple[str, ...]]:
    """
    Предразрешение метаданных Phoenix на весь запуск:
      - имя основной таблицы (PHX_TABLE/PHOENIX_TABLE/HBASE_MAIN_TABLE/HBASE_TABLE),
      - название столбца-времени (HBASE_MAIN_TS_COLUMN, по умолчанию 'tm'),
      - список колонок для выборки (если не задан в конфиге — используем CH_COLUMNS).

    Возвращает кортеж (table, ts_col, columns).
    """
    table: Optional[str] = None
    for name in ("PHX_TABLE", "PHOENIX_TABLE", "HBASE_MAIN_TABLE", "HBASE_TABLE", "PHX_MAIN_TABLE"):
        try:
            val = getattr(cfg, name, None)
            if val:
                table = str(val)
                break
        except Exception:
            continue
    if not table:
        raise ValueError("Не задано имя таблицы Phoenix в конфиге (ожидали PHX_TABLE/HBASE_MAIN_TABLE и т.п.).")

    try:
        ts_col = str(getattr(cfg, "HBASE_MAIN_TS_COLUMN", "tm") or "tm")
    except Exception:
        ts_col = "tm"

    phx_cols_cfg = getattr(cfg, "PHX_COLUMNS", None)
    if phx_cols_cfg:
        if isinstance(phx_cols_cfg, (list, tuple)):
            phx_cols = tuple(str(c) for c in phx_cols_cfg)
        elif isinstance(phx_cols_cfg, str):
            phx_cols = tuple(s.strip() for s in phx_cols_cfg.split(",") if s.strip())
        else:
            phx_cols = tuple(CH_COLUMNS)
    else:
        phx_cols = tuple(CH_COLUMNS)

    return table, ts_col, phx_cols


def process_one_slice(
    *,
    cfg: Any,
    phx: PhoenixLike,
    ch_table_raw_all: str,
    ch_batch: int,
    s_q: datetime,
    e_q: datetime,
    phx_meta: Tuple[str, str, Sequence[str]],
    row_to_tuple: Callable[[Dict[str, Any]], tuple] = _row_to_ch_tuple,
    opd_to_part: Callable[[Any], Optional[int]] = _opd_to_part_utc,
    insert_rows: Callable[[str, List[tuple], Tuple[str, ...]], int] = lambda t, r, c: 0,
    maybe_hb: Optional[Callable[[int, int], None]] = None,
    new_rows_since_pub_by_part: Optional[Dict[int, int]] = None,
    pending_parts: Optional[Set[int]] = None,
) -> Tuple[int, int]:
    """
    Совместимый с оркестратором адаптер:
    - читает батчи из Phoenix,
    - конвертирует в кортежи,
    - пишет в RAW через переданный колбэк insert_rows (клиент CH скрыт в вызывающей стороне),
    - обновляет pending_parts и счётчики new_rows_since_pub_by_part.

    Возвращает (rows_read, rows_written) за слайс.
    """
    table, ts_col, columns = phx_meta

    if new_rows_since_pub_by_part is None:
        new_rows_since_pub_by_part = {}
    if pending_parts is None:
        pending_parts = set()

    rows_read = 0
    rows_written = 0
    buf: List[tuple] = []

    # Локальные ссылки
    fetch_iter = phx.fetch_increment_adaptive
    append = buf.append
    add_part = pending_parts.add
    get_count = new_rows_since_pub_by_part.get
    set_count = new_rows_since_pub_by_part.__setitem__
    columns_ch: Tuple[str, ...] = tuple(CH_COLUMNS)

    for batch in fetch_iter(table, ts_col, columns, s_q, e_q):
        if not batch:
            continue

        for r in batch:
            append(row_to_tuple(r))
            p = opd_to_part(r.get("opd"))
            if p is not None:
                add_part(p)
                set_count(p, (get_count(p, 0) + 1))

        rows_read += len(batch)

        if len(buf) >= ch_batch:
            rows_written += int(insert_rows(ch_table_raw_all, buf, columns_ch))
            buf.clear()
            if maybe_hb:
                maybe_hb(rows_read, rows_written)

    if buf:
        rows_written += int(insert_rows(ch_table_raw_all, buf, columns_ch))
        buf.clear()
        if maybe_hb:
            maybe_hb(rows_read, rows_written)

    return rows_read, rows_written


# -----------------------------------------------------------------------------
# Публичные псевдонимы (для импортов без подчёркивания)
# -----------------------------------------------------------------------------
row_to_ch_tuple = _row_to_ch_tuple
opd_to_part_utc = _opd_to_part_utc


# Публичный интерфейс модуля
__all__ = [
    # публичные имена
    "process_one_slice",
    "row_to_ch_tuple",
    "opd_to_part_utc",
    "resolve_phx_table_and_cols",
    # приватные (для обратной совместимости и прямых вызовов)
    "_row_to_ch_tuple",
    "_flush_ch_buffer",
    "_proc_phx_batch",
    "_process_one_slice",
    "_opd_to_part_utc",
    "_parse_ch_storage",
    # константы схемы
    "CH_COLUMNS",
    "CH_DT_FIELDS",
    "CH_INT_FIELDS",
]