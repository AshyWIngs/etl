# file: scripts/publishing.py
# -*- coding: utf-8 -*-
"""
Логика публикации (дедуп) партиций из RAW в CLEAN для ClickHouse.

Ответственность модуля:
- принимать список (множество) партиций YYYYMMDD;
- выполнять дедуп в буфере (argMax по ingested_at) и REPLACE PARTITION в целевой таблице;
- делать это атомарно для каждой партиции, c аккуратными логами и best-effort дневником.

Схема колонок, список выбираемых полей и выражения агрегирования вынесены в `schema.py`.
Если `schema.py` недоступен, используется безопасный fallback (его можно удалить
как только schema.py появится в репозитории).
"""

from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, TYPE_CHECKING, cast

log = logging.getLogger("codes_history_increment")

# --- Журнал запусков (best effort на этапе публикации) ---
from .journal import ProcessJournal
from .config import Settings

# --- Схема: предпочитаем из schema.py; иначе fallback ---
try:
    # Рекомендуемый путь: схема/агрегирующие выражения живут отдельно
    from .schema import CH_COLUMNS_STR, DEDUP_SELECT_COLS  # type: ignore[attr-defined]
except Exception:
    # Временная подстраховка — не для долгой жизни.
    log.debug("publishing.py: fallback-схема активирована (schema.py пока отсутствует).")
    CH_COLUMNS = (
        "c","t","opd",
        "id","did",
        "rid","rinn","rn","sid","sinn","sn","gt","prid",
        "st","ste","elr",
        "emd","apd","exd",
        "p","pt","o","pn","b",
        "tt","tm",
        "ch","j",
        "pg","et",
        "pvad","ag"
    )
    CH_COLUMNS_STR = ", ".join(CH_COLUMNS)
    _NON_KEY = [c for c in CH_COLUMNS if c not in ("c", "t", "opd")]
    _DEDUP_AGG = ",\n              ".join([f"argMax({c}, ingested_at) AS {c}" for c in _NON_KEY])
    DEDUP_SELECT_COLS = "c, t, opd,\n              " + _DEDUP_AGG


# --- Типы клиентов — мягкая типизация, чтобы редакторы подсказывали сигнатуры ---
if TYPE_CHECKING:
    class CHClientLike:
        def execute(self, sql: str) -> Optional[List[tuple]]: ...
    class PGClientLike:
        def close(self) -> None: ...
else:
    CHClientLike = Any
    PGClientLike = Any


# ------------------------------- Вспомогательные хелперы -------------------------------

def _parts_to_interval(parts: Iterable[int]) -> Tuple[datetime, datetime]:
    """
    По набору YYYYMMDD → (UTC-начало, UTC-конец последнего дня).
    Нужна только для записи в журнал (mark_running/mark_done), вне «горячего» пути.
    """
    ps = sorted(set(int(p) for p in parts))
    if not ps:
        now = datetime.now(timezone.utc)
        return now, now
    p0, p1 = ps[0], ps[-1]
    y0, m0, d0 = p0 // 10000, (p0 // 100) % 100, p0 % 100
    y1, m1, d1 = p1 // 10000, (p1 // 100) % 100, p1 % 100
    start = datetime(y0, m0, d0, tzinfo=timezone.utc)
    end   = datetime(y1, m1, d1, tzinfo=timezone.utc) + timedelta(days=1)
    return start, end


def _on_cluster_suffix(cfg: Settings) -> str:
    """Возвращает ' ON CLUSTER <name>' либо пустую строку."""
    try:
        cluster = getattr(cfg, "CH_CLUSTER", None)
        return f" ON CLUSTER {cluster}" if cluster else ""
    except Exception:
        return ""


def log_gating_debug(
    *,
    pending_parts: Set[int],
    new_rows_by_part: Dict[int, int],
    publish_min_new_rows: int,
    slices_since_last_pub: int,
    publish_every_slices: int,
) -> None:
    """Диагностический DEBUG-лог причин гейтинга."""
    if not log.isEnabledFor(logging.DEBUG):
        return
    if not pending_parts:
        log.debug(
            "Гейтинг публикации: pending_parts=∅, slices=%d/%d, threshold=%d",
            slices_since_last_pub, publish_every_slices, publish_min_new_rows,
        )
        return
    parts_sorted = sorted(pending_parts)
    rows_info = ", ".join(f"{p}:{new_rows_by_part.get(p, 0)}" for p in parts_sorted)
    log.debug(
        "Гейтинг публикации: pending_parts=%s | rows_since_pub={%s} | slices=%d/%d | threshold=%d",
        parts_sorted, rows_info, slices_since_last_pub, publish_every_slices, publish_min_new_rows,
    )


def select_parts_to_publish(
    *,
    pending_parts: Set[int],
    publish_only_if_new: bool,
    publish_min_new_rows: int,
    new_rows_by_part: Dict[int, int],
) -> Set[int]:
    """
    Выбирает партиции для публикации:
    - если publish_only_if_new=False → публикуем все pending_parts;
    - иначе публикуем только те, где набралось ≥ publish_min_new_rows новых строк.
    """
    if not pending_parts:
        return set()
    if not publish_only_if_new:
        return set(pending_parts)
    return {p for p in pending_parts if new_rows_by_part.get(p, 0) >= publish_min_new_rows}


# ------------------------------- Основная публикация (дедуп) -------------------------------

def publish_parts(
    ch: CHClientLike,
    cfg: Settings,
    pg: PGClientLike,
    process_name: str,
    parts: Set[int],
) -> bool:
    """
    Публикует набор партиций из RAW в CLEAN с дедупом:

      1) DROP BUF (если есть хвост от прошлого запуска);
      2) INSERT INTO BUF SELECT ... FROM RAW GROUP BY c, t, opd
         (значения колонок — argMax(..., ingested_at));
      3) REPLACE PARTITION <p> FROM BUF в CLEAN;
      4) DROP BUF (всегда, как бы ни прошёл REPLACE).

    Возвращает True, если была выполнена хотя бы одна REPLACE PARTITION.
    """
    if not parts:
        return False

    execute: Callable[[str], Optional[List[tuple]]] = ch.execute
    on_cluster = _on_cluster_suffix(cfg)

    raw_all = cfg.CH_RAW_TABLE
    clean   = cfg.CH_CLEAN_TABLE
    buf     = cfg.CH_DEDUP_BUF_TABLE

    # Журнал для этапа дедупа (best-effort: ошибки журнала не должны ломать публикацию)
    dedup_journal: Optional[ProcessJournal] = None
    try:
        dedup_journal = ProcessJournal(cast(Any, pg), cfg.JOURNAL_TABLE, f"{process_name}:dedup")  # type: ignore[arg-type]
        dedup_journal.ensure()
    except Exception:
        dedup_journal = None

    pub_start, pub_end = _parts_to_interval(parts)

    try:
        if dedup_journal:
            dedup_journal.mark_running(pub_start, pub_end, host=socket.gethostname(), pid=os.getpid())
    except Exception:
        pass

    published_any = False
    parts_sorted = sorted(set(int(p) for p in parts))

    for p in parts_sorted:
        try:
            # 1) Страховочный DROP буферной партиции
            try:
                execute(f"ALTER TABLE {buf}{on_cluster} DROP PARTITION {p}")
            except Exception:
                pass  # не критично

            # 2) Дедуп в буфере
            insert_sql = (
                f"INSERT INTO {buf} ({CH_COLUMNS_STR}) "
                f"SELECT {DEDUP_SELECT_COLS} "
                f"FROM {raw_all} "
                f"WHERE toYYYYMMDD(opd) = {p} "
                f"GROUP BY c, t, opd"
            )
            execute(insert_sql)

            # 3) Публикация
            replace_sql = f"ALTER TABLE {clean}{on_cluster} REPLACE PARTITION {p} FROM {buf}"
            execute(replace_sql)
            published_any = True

        except Exception as ex:
            log.warning("Публикация партиции %s пропущена: %s", p, ex, exc_info=log.isEnabledFor(logging.DEBUG))
        finally:
            # 4) Финальная уборка буфера — при любом исходе
            try:
                execute(f"ALTER TABLE {buf}{on_cluster} DROP PARTITION {p}")
            except Exception:
                pass

    try:
        if dedup_journal:
            dedup_journal.mark_done(pub_start, pub_end, rows_read=0, rows_written=0)
    except Exception:
        pass

    return published_any


# Обратная совместимость со старым именем
_publish_parts = publish_parts


# ------------------------------- Финальная публикация / бэкфилл -------------------------------

def _dt_to_dt64_utc_str(dt: datetime) -> str:
    """ClickHouse-совместимая строка (секунды достаточно)."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parts_in_window(ch: CHClientLike, table: str, start: datetime, end: datetime) -> Set[int]:
    """DISTINCT партиции (toYYYYMMDD(opd)) из RAW за окно [start, end)."""
    execute: Callable[[str], Optional[List[tuple]]] = ch.execute
    s = _dt_to_dt64_utc_str(start)
    e = _dt_to_dt64_utc_str(end)
    sql = (
        f"SELECT DISTINCT toYYYYMMDD(opd) AS p "
        f"FROM {table} "
        f"WHERE opd >= toDateTime64('{s}', 3, 'UTC') AND opd < toDateTime64('{e}', 3, 'UTC')"
    )
    rows = execute(sql) or []
    return {int(r[0]) for r in rows if r and r[0] is not None}


def perform_startup_backfill(
    *,
    ch: CHClientLike,
    cfg: Settings,
    until_dt: datetime,
    process_name: str,
    pg: PGClientLike,
) -> None:
    """
    Лёгкий стартовый бэкфилл: добираем недопубликованные партиции
    за последние STARTUP_BACKFILL_DAYS (по умолчанию 0 — выключено).
    """
    try:
        days = int(getattr(cfg, "STARTUP_BACKFILL_DAYS", 0) or 0)
    except Exception:
        days = 0
    if days <= 0:
        return

    try:
        start = (until_dt - timedelta(days=days)).replace(tzinfo=timezone.utc)
        raw = cfg.CH_RAW_TABLE
        parts = _parts_in_window(ch, raw, start, until_dt)
        if not parts:
            return
        log.info("Стартовый бэкфилл: найдено партиций за последние %d дн.: %s", days, ", ".join(map(str, sorted(parts))))
        publish_parts(ch, cfg, pg, process_name, parts)
    except Exception as ex:
        log.warning("Стартовый бэкфилл пропущен: %s", ex, exc_info=log.isEnabledFor(logging.DEBUG))


def finalize_publication(
    *,
    ch: CHClientLike,
    cfg: Settings,
    pg: PGClientLike,
    process_name: str,
    pending_parts: Set[int],
    since_dt: datetime,
    until_dt: datetime,
    backfill_missing_enabled: bool,
    new_rows_since_pub_by_part: Dict[int, int],
) -> None:
    """
    Финальная публикация в конце окна:
    - публикуем все накопленные pending_parts без гейтинга (ALWAYS_PUBLISH_AT_END);
    - при backfill_missing_enabled=True дополнительно публикуем все партиции,
      попавшие в окно [since_dt, until_dt) (best-effort).
    """
    to_publish: Set[int] = set(int(p) for p in pending_parts)

    if backfill_missing_enabled:
        try:
            extra = _parts_in_window(ch, cfg.CH_RAW_TABLE, since_dt.replace(tzinfo=timezone.utc), until_dt.replace(tzinfo=timezone.utc))
            if extra:
                to_publish |= extra
        except Exception as ex:
            log.warning("Финальный добор партиций пропущен: %s", ex, exc_info=log.isEnabledFor(logging.DEBUG))

    if not to_publish:
        log.info("Финальная публикация: нечего публиковать.")
        return

    log.info("Финальная публикация: партиций=%d (%s)", len(to_publish), ", ".join(map(str, sorted(to_publish))))
    publish_parts(ch, cfg, pg, process_name, to_publish)


__all__ = [
    # публичные функции
    "publish_parts",
    "select_parts_to_publish",
    "log_gating_debug",
    "perform_startup_backfill",
    "finalize_publication",
    "publish_parts",
]