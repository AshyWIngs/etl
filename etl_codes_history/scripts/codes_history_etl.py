# file: scripts/codes_history_etl.py
# -*- coding: utf-8 -*-
"""
Incremental ETL: Phoenix(HBase) → ClickHouse (native 9000, с компрессией).

Цели: простота, предсказуемость, высокая производительность и отказоустойчивость.

Ключевое поведение
------------------
• Поля переносим «как есть» (без вычислений id/md5, без sys-атрибутов).
• Дедуп на публикации по ключу (c, t, opd) через argMax(..., ingested_at).
• Окно запроса в Phoenix совпадает с бизнес-окном слайса; при желании можно добавить
  «захлёст» назад PHX_QUERY_OVERLAP_MINUTES только на первом слайсе.
  Правую границу не «замораживаем».
• DateTime64(3) передаём как python datetime (naive), микросекунды обрезаем до миллисекунд.
• Столбец `ingested_at` в ClickHouse не передаём в INSERT — он заполняется `DEFAULT now('UTC')`.
  Это устраняет несоответствие числа колонок и не влияет на дедуп (агрегация идёт по `ingested_at` в RAW).
• Никаких CSV — сразу INSERT в ClickHouse (native, compression).

Каденс публикаций
-----------------
• Публикация управляется ТОЛЬКО числом успешных слайсов: PUBLISH_EVERY_SLICES.
• В конце запуска всегда выполняется финальная публикация (ALWAYS_PUBLISH_AT_END зафиксирован).
• Для защиты от «пустых» публикаций действует лёгкий гейтинг: считаем «новые» строки в памяти
  по каждой партиции в рамках текущего запуска и публикуем партицию только если новых строк
  ≥ PUBLISH_MIN_NEW_ROWS (когда PUBLISH_ONLY_IF_NEW=1). В manual-режиме гейтинг отключён.

Как это работает
----------------
1) Разбиваем окно [--since, --until) на слайды по STEP_MIN минут (UTC-времена формируются из CLI; наивные значения трактуются в BUSINESS_TZ (если задана), иначе — в UTC).
2) Для каждого слайда:
   – фиксируем planned→running в журнале;
   – читаем Phoenix батчами (PHX_FETCHMANY_SIZE), сортировка по времени;
   – нормализуем типы и буферизуем; при достижении CH_INSERT_BATCH — INSERT VALUES в RAW.
3) Накапливаем список партиций (toYYYYMMDD(opd) по UTC), задетых слайсом.
4) По достижении порога PUBLISH_EVERY_SLICES выполняем публикацию каждой партиции:
   DROP BUF → INSERT BUF (GROUP BY ... argMax) → REPLACE CLEAN → DROP BUF.
   Всё замеряется и логируется; создаётся запись в журнале `<PROCESS_NAME>:dedup`.
5) По завершении окна выполняем финальную публикацию (если включена).

Надёжность
----------
• Один активный инстанс (advisory-lock) + очистка битых planned/running.
• Мягкий retry CH при UnexpectedPacketFromServerError: reconnect() + повтор.
• Ранний контроль наличия требуемых таблиц ClickHouse (EXISTS TABLE).
• Понятные стартовые ошибки с привязкой к окну запуска в журнале.

CLI (минимальный)
------------------
--since / --until      — окно обработки от и до. --since можно опустить: будет взят watermark из inc_processing_state; --until можно опустить: тогда until = since + RUN_WINDOW_HOURS (по умолчанию 24 ч).
--manual-start         — ручной запуск (журнал: manual_<PROCESS_NAME>).
                         В manual-режиме дополнительно:
                         • публикация выполняется всегда (гейтинг по «новизне» отключён),
                         • в начале запуска выполняется лёгкий backfill пропущенных дней за окно запуска.
--log-level            — уровень логирования (по умолчанию INFO).
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import signal
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, cast, TYPE_CHECKING

# psycopg UniqueViolation — для защиты mark_planned от гонки
try:
    from psycopg.errors import UniqueViolation  # type: ignore[reportMissingImports]
except Exception:  # pragma: no cover
    class UniqueViolation(Exception):  # type: ignore[misc]
        pass

# Настройка логов/конфиг/диагностика
from .logging_setup import setup_logging
from .config import Settings
from .diag import want_trace, log_maybe_trace, log_window_hints

# Выбор слайсера (UTC-сетка предпочтительнее)
try:
    from .slicer import iter_slices_grid as _iter_slices_impl  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    from .slicer import iter_slices as _iter_slices_impl

# Клиенты БД и журнал
from .db.phoenix_client import PhoenixClient
from .db.pg_client import PGClient, PGConnectionError
from .db.clickhouse_client import ClickHouseClient as CHClient
from .journal import ProcessJournal

# --- Импорты из вынесенных модулей (слои ответственности) ---

from .publishing import (
    publish_parts,
    finalize_publication,
    perform_startup_backfill,
    select_parts_to_publish,
    log_gating_debug,
)

# Слайсинг/обработка (горячий путь)
from .slices import (
    process_one_slice,
    row_to_ch_tuple,
    opd_to_part_utc,
    resolve_phx_table_and_cols,
)

# Бутстрап/инициализация (PG/оконный контекст/подключения)
from .bootstrap import (
    connect_pg_and_journal,
    startup_fail_with_journal,
    connect_phx_client,
    connect_ch_client,
    ensure_ch_tables_or_fail,
    required_ch_tables,
    bootstrap_pg_and_window,
    init_backends_and_check,
)

# Время/TZ и расчёт границ слайса для Phoenix
from .timeutils import (
    compute_phx_slice_and_log,
)

log = logging.getLogger("codes_history_increment")

# ------------------------ Утилиты и сигнал-менеджмент ------------------------

def _reduce_noise_for_info_mode() -> None:
    """При уровне INFO приглушаем «болтливые» под-логгеры."""
    try:
        main_logger = logging.getLogger("codes_history_increment")
        if main_logger.isEnabledFor(logging.DEBUG):
            return
        for name in (
            "scripts.db.phoenix_client",
            "phoenixdb",
            "urllib3.connectionpool",
            "scripts.journal",
        ):
            try:
                logging.getLogger(name).setLevel(logging.WARNING)
            except Exception:
                pass
    except Exception:
        pass


# Грейсфул-остановка по сигналам ОС
from threading import Event
shutdown_event = Event()
_shutdown_reason = ""

def _install_signal_handlers() -> None:
    def _handle(signum, frame):
        global _shutdown_reason
        try:
            name = signal.Signals(signum).name
        except Exception:
            name = str(signum)
        _shutdown_reason = name
        log.warning("Получен сигнал %s — инициирую мягкую остановку (graceful)...", name)
        shutdown_event.set()

    for _sig in (getattr(signal, "SIGINT", None),
                 getattr(signal, "SIGTERM", None),
                 getattr(signal, "SIGQUIT", None),
                 getattr(signal, "SIGHUP", None)):
        if _sig is not None:
            try:
                signal.signal(_sig, _handle)
            except Exception:
                pass

def _interrupt_message() -> str:
    if shutdown_event.is_set() and _shutdown_reason != "SIGINT":
        return f"Остановлено сигналом ({_shutdown_reason})"
    return "Остановлено пользователем (SIGINT)"

def _check_stop() -> None:
    if shutdown_event.is_set():
        raise KeyboardInterrupt()

# ------------------------ Параметры выполнения (для S107) ---------------------

@dataclass(frozen=True, slots=True)
class ExecParams:
    """Набор неизменяемых параметров выполнения основного цикла."""
    journal: "ProcessJournal"
    cfg: "Settings"
    ch: "CHClient"
    phx: "PhoenixClient"
    pg: "PGClient"
    process_name: str
    host: str
    pid: int
    since_dt: datetime
    until_dt: datetime
    step_min: int
    business_tz_name: str
    overlap_delta: timedelta
    publish_every_slices: int
    publish_only_if_new: bool
    publish_min_new_rows: int
    always_publish_at_end: bool
    backfill_missing_enabled: bool
    # Предразрешённые метаданные Phoenix (фиксированы на весь запуск)
    phx_table: str
    phx_ts_col: str
    phx_cols: Tuple[str, ...]

# ------------------------ Оркестрация: публикация по порогу -------------------

def _maybe_intermediate_publish(
    *,
    pending_parts: Set[int],
    new_rows_by_part: Dict[int, int],
    publish_every_slices: int,
    slices_since_last_pub: int,
    publish_only_if_new: bool,
    publish_min_new_rows: int,
    ch: "CHClient",
    cfg: "Settings",
    pg: "PGClient",
    process_name: str,
    after_publish: Callable[[Set[int]], None],
) -> bool:
    """Промежуточная публикация при достижении порога слайсов."""
    if publish_every_slices <= 0 or slices_since_last_pub < publish_every_slices:
        return False

    log_gating_debug(
        pending_parts=pending_parts,
        new_rows_by_part=new_rows_by_part,
        publish_min_new_rows=publish_min_new_rows,
        slices_since_last_pub=slices_since_last_pub,
        publish_every_slices=publish_every_slices,
    )

    parts_to_publish = select_parts_to_publish(
        pending_parts=pending_parts,
        publish_only_if_new=publish_only_if_new,
        publish_min_new_rows=publish_min_new_rows,
        new_rows_by_part=new_rows_by_part,
    )
    if not parts_to_publish:
        log.info(
            "Промежуточная публикация отложена гейтингом (порог=%d, слайсы=%d/%d).",
            publish_min_new_rows, slices_since_last_pub, publish_every_slices,
        )
        return False

    if publish_parts(ch, cfg, pg, process_name, parts_to_publish):
        log.info("Промежуточная публикация: %d партиций.", len(parts_to_publish))
        after_publish(parts_to_publish)
        return True
    return False

# ------------------------ Оркестрация: heartbeat и план -----------------------

def _plan_run_with_heartbeat(
    journal: "ProcessJournal",
    s: datetime,
    e: datetime,
    host: str,
    pid: int,
    hb_interval_sec: int,
    total_written_ch: int,
) -> Tuple[int, Callable[[int, int], None]]:
    """
    Планирует слайс (с санацией конфликтующих planned), переводит в running и
    возвращает (run_id, maybe_hb).
    """
    mark_planned = journal.mark_planned
    mark_running = journal.mark_running
    _clear_planned = getattr(journal, "clear_conflicting_planned", None)

    try:
        if callable(_clear_planned):
            _clear_planned(s, e)
    except Exception:
        pass
    try:
        mark_planned(s, e)
    except UniqueViolation:
        if callable(_clear_planned):
            _clear_planned(s, e)
        mark_planned(s, e)

    run_id = mark_running(s, e, host=host, pid=pid)

    if hb_interval_sec < 5:
        hb_interval_sec = 5
    _next_deadline = perf_counter() + hb_interval_sec

    try:
        journal.heartbeat(run_id, progress={"rows_read": 0, "rows_written": total_written_ch})
    except Exception:
        pass

    def maybe_hb(rows_read: int, total_written: int) -> None:
        nonlocal _next_deadline
        nowp = perf_counter()
        if nowp >= _next_deadline:
            try:
                journal.heartbeat(run_id, progress={"rows_read": rows_read, "rows_written": total_written})
            except Exception:
                pass
            _next_deadline = nowp + hb_interval_sec

    return run_id, maybe_hb

# ------------------------ Оркестрация: один слайс -----------------------------

def _run_one_slice_and_maybe_publish(
    params: ExecParams,
    s: datetime,
    e: datetime,
    *,
    ch_table_raw_all: str,
    ch_batch: int,
    logged_tz_context: bool,
    is_first_slice: bool,
    slices_since_last_pub: int,
    total_written_ch: int,
    pending_parts: Set[int],
    new_rows_since_pub_by_part: Dict[int, int],
    after_publish: Callable[[Set[int]], None],
) -> Tuple[int, int, bool, bool, int, int]:
    """
    Выполняет цикл обработки одного слайса: планирование, чтение Phoenix,
    batch-вставки в RAW, промежуточную публикацию и mark_done.
    Возвращает:
        rows_read, written_now, is_first_slice, logged_tz_context, slices_since_last_pub, total_written_ch
    """
    compute_slice: Callable[[datetime, datetime, bool, timedelta, str, bool], Tuple[datetime, datetime, bool]] = _compute_phx_slice_and_log
    plan_run: Callable[[ProcessJournal, datetime, datetime, str, int, int, int], Tuple[int, Callable[[int, int], None]]] = _plan_run_with_heartbeat
    process_slice: Callable[..., Tuple[int, int]] = _process_one_slice
    intermediate_publish: Callable[..., bool] = _maybe_intermediate_publish
    journal = params.journal
    cfg = params.cfg
    ch = params.ch
    pg = params.pg
    proc_name = params.process_name
    log_local = log

    # Методы журнала (горячий путь)
    mark_done: Callable[..., Optional[int]] = journal.mark_done
    mark_error: Callable[..., Optional[int]] = journal.mark_error
    heartbeat: Callable[..., None] = journal.heartbeat
    mark_cancelled: Optional[Callable[..., Optional[int]]] = getattr(journal, "mark_cancelled", None)

    # 1) Окно Phoenix + разовый лог TZ-контекста
    s_q, e_q, logged_tz_context = compute_slice(
        s,
        e,
        is_first_slice,
        params.overlap_delta,
        params.business_tz_name,
        logged_tz_context,
    )

    # 2) Планирование слайса + heartbeat
    hb_interval_sec = int(getattr(cfg, "JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", 300) or 300)
    run_id, maybe_hb = plan_run(
        journal,
        s,
        e,
        params.host,
        params.pid,
        hb_interval_sec,
        total_written_ch,
    )

    # 3) Основная работа: чтение Phoenix → нормализация → INSERT в CH
    try:
        insert_rows: Callable[[str, List[tuple], Tuple[str, ...]], int] = ch.insert_rows
        rows_read, written_now = process_slice(
            cfg=cfg,
            phx=params.phx,
            ch_table_raw_all=ch_table_raw_all,
            ch_batch=ch_batch,
            s_q=s_q,
            e_q=e_q,
            phx_meta=(params.phx_table, params.phx_ts_col, params.phx_cols),
            row_to_tuple=_row_to_ch_tuple,
            opd_to_part=_opd_to_part_utc,
            insert_rows=insert_rows,
            maybe_hb=maybe_hb,
            new_rows_since_pub_by_part=new_rows_since_pub_by_part,
            pending_parts=pending_parts,
        )
        total_written_ch = total_written_ch + written_now
        slices_since_last_pub += 1
        is_first_slice = False

        # 4) Промежуточная публикация ДО mark_done()
        intermediate_publish(
            pending_parts=pending_parts,
            new_rows_by_part=new_rows_since_pub_by_part,
            publish_every_slices=params.publish_every_slices,
            slices_since_last_pub=slices_since_last_pub,
            publish_only_if_new=params.publish_only_if_new,
            publish_min_new_rows=params.publish_min_new_rows,
            ch=ch, cfg=cfg, pg=pg, process_name=proc_name,
            after_publish=after_publish,
        )

        # 5) Закрываем слайс
        mark_done(s, e, rows_read=rows_read, rows_written=total_written_ch)
        _pp = ",".join(str(p) for p in sorted(pending_parts)) or "-"
        log_local.info(
            "Слайс завершён: %s → %s (PHX: %s → %s); rows_read=%d, rows_written_raw_total=%d, pending_parts=%s",
            s.isoformat(),
            e.isoformat(),
            s_q.isoformat(),
            e_q.isoformat(),
            rows_read,
            total_written_ch,
            _pp,
        )
        return rows_read, written_now, is_first_slice, logged_tz_context, slices_since_last_pub, total_written_ch

    except KeyboardInterrupt:
        try:
            if callable(mark_cancelled):
                mark_cancelled(s, e, message=_interrupt_message())
            else:
                mark_error(s, e, message=_interrupt_message())
        except Exception:
            pass
        raise
    except Exception as ex:
        # Фиксируем прогресс и ошибку; пробрасываем наверх
        heartbeat(run_id, progress={"error": str(ex)})
        mark_error(s, e, message=str(ex))
        raise

# ------------------------ Оркестрация: основной цикл под lock -----------------

def _execute_with_lock(params: ExecParams) -> Tuple[int, int]:
    """
    Основной сценарий выполнения под advisory-lock из журнала.
    Возвращает (total_read, total_written_ch).
    """
    journal = params.journal
    cfg = params.cfg
    ch = params.ch
    pg = params.pg
    process_name = params.process_name
    since_dt = params.since_dt
    until_dt = params.until_dt
    step_min = params.step_min
    always_publish_at_end = params.always_publish_at_end
    backfill_missing_enabled = params.backfill_missing_enabled

    exclusive_lock: Callable[[], object] = journal.exclusive_lock  # контекстный менеджер
    sanitize_stale: Callable[..., None] = journal.sanitize_stale
    iter_slices: Callable[[datetime, datetime, int], Iterable[Tuple[datetime, datetime]]] = _iter_slices_impl
    finalize_pub: Callable[..., None] = finalize_publication
    run_slice = _run_one_slice_and_maybe_publish
    check_stop = _check_stop

    total_read = 0
    total_written_ch = 0
    pending_parts: Set[int] = set()
    slices_since_last_pub = 0
    new_rows_since_pub_by_part: Dict[int, int] = defaultdict(int)

    ch_table_raw_all = cfg.CH_RAW_TABLE
    ch_batch = int(cfg.CH_INSERT_BATCH)

    def _after_publish_update_state(published: Set[int]) -> None:
        nonlocal slices_since_last_pub
        for p in published:
            new_rows_since_pub_by_part[p] = 0
            pending_parts.discard(p)
        slices_since_last_pub = 0

    try:
        with exclusive_lock() as got:
            if not got:
                log.warning("Другой инстанс '%s' уже выполняется — выходим.", process_name)
                return total_read, total_written_ch

            # Санация «зависших» запусков
            sanitize_stale(
                planned_ttl_minutes=60,
                running_heartbeat_timeout_minutes=45,
                running_hard_ttl_hours=12,
            )

            # Лёгкий стартовый бэкфилл недопубликованных партиций
            perform_startup_backfill(ch=ch, cfg=cfg, until_dt=until_dt, process_name=process_name, pg=pg)

            logged_tz_context = False
            is_first_slice = True

            for s, e in iter_slices(since_dt, until_dt, step_min):
                check_stop()
                rows_read, _, is_first_slice, logged_tz_context, slices_since_last_pub, total_written_ch = run_slice(
                    params,
                    s,
                    e,
                    ch_table_raw_all=ch_table_raw_all,
                    ch_batch=ch_batch,
                    logged_tz_context=logged_tz_context,
                    is_first_slice=is_first_slice,
                    slices_since_last_pub=slices_since_last_pub,
                    total_written_ch=total_written_ch,
                    pending_parts=pending_parts,
                    new_rows_since_pub_by_part=new_rows_since_pub_by_part,
                    after_publish=_after_publish_update_state,
                )
                total_read += rows_read

            # Финальная публикация (и опциональный добор пропущенных партиций)
            if always_publish_at_end:
                finalize_pub(
                    ch=ch,
                    cfg=cfg,
                    pg=pg,
                    process_name=process_name,
                    pending_parts=pending_parts,
                    since_dt=since_dt,
                    until_dt=until_dt,
                    backfill_missing_enabled=backfill_missing_enabled,
                    new_rows_since_pub_by_part=new_rows_since_pub_by_part,
                )
    finally:
        # Пустой finally для корректной формы try-блока.
        pass
    return total_read, total_written_ch

# ------------------------ Обвязка запуска -------------------------------------

def _close_quietly(*resources: object) -> None:
    """Закрывает переданные объекты, если у них есть close(). Исключения подавляются."""
    for r in resources:
        if r is None:
            continue
        try:
            close = getattr(r, "close", None)
            if callable(close):
                close()
        except Exception:
            pass

def _resolve_process_name(cfg: "Settings", manual: bool) -> str:
    base = str(getattr(cfg, "PROCESS_NAME", ""))
    return ("manual_" + base) if manual else base

def _make_startup_fail_handler(
    journal: "ProcessJournal",
    since_dt: datetime,
    until_dt: datetime,
    cfg: "Settings",
):
    """Фабрика колбэка для единообразной обработки стартовых ошибок компонентов."""
    def _on_fail(component: str, exc: Exception, extra: Optional[Dict[str, Any]] = None) -> None:
        _startup_fail_with_journal(journal, since_dt, until_dt, cfg, component, exc, extra)
    return _on_fail

# ------------------------ Вынесенный heavy ETL-процесс ------------------------

def _run_etl_impl(args: argparse.Namespace) -> None:
    """Вся тяжёлая оркестрация запуска."""
    setup_logging(args.log_level)
    _reduce_noise_for_info_mode()
    if getattr(args, "profile", False):
        os.environ["ETL_PROFILE"] = "1"
    cfg = Settings()
    _install_signal_handlers()

    manual_mode = bool(args.manual_start)
    process_name = _resolve_process_name(cfg, manual_mode)
    backfill_missing_enabled = manual_mode  # добор пропущенных партиций только в ручном режиме

    phx: Optional[PhoenixClient] = None
    ch: Optional[CHClient] = None
    pg: Optional[PGClient] = None
    try:
        # PG + журнал + окно + on_startup_fail
        (
            pg,
            journal,
            since_dt,
            until_dt,
            used_watermark,
            auto_until,
            auto_window_hours,
            business_tz_name,
            on_startup_fail,
        ) = _bootstrap_pg_and_window(cfg, args, process_name)
        _log_window_hints(used_watermark, auto_until, auto_window_hours)

        # Подключения к Phoenix/CH и ранняя проверка таблиц
        phx, ch = _init_backends_and_check(cfg, on_startup_fail)

        # Политика публикации (инлайн — быстрее и проще)
        publish_every_slices = int(getattr(cfg, "PUBLISH_EVERY_SLICES", 0) or 0)
        always_publish_at_end = True
        publish_only_if_new = bool(int(getattr(cfg, "PUBLISH_ONLY_IF_NEW", 1))) and (not manual_mode)
        publish_min_new_rows = int(getattr(cfg, "PUBLISH_MIN_NEW_ROWS", 1))

        # Захлёст первого слайса для Phoenix
        try:
            _m = int(getattr(cfg, "PHX_QUERY_OVERLAP_MINUTES", 0) or 0)
        except Exception:
            _m = 0
        overlap_delta = timedelta(minutes=_m) if _m > 0 else timedelta(0)

        # Шаг слайсера
        try:
            step_min = int(getattr(cfg, "STEP_MIN", 60))
        except Exception:
            step_min = 60

        host = socket.gethostname()
        pid = os.getpid()

        # Предразрешаем Phoenix-метаданные (таблица/TS-столбец/набор колонок)
        phx_table, phx_ts_col, phx_cols = _resolve_phx_table_and_cols(cfg)

        # Основной сценарий под журнал-lock
        params = ExecParams(
            journal=journal,
            cfg=cfg,
            ch=ch,
            phx=phx,
            pg=pg,
            process_name=process_name,
            host=host,
            pid=pid,
            since_dt=since_dt,
            until_dt=until_dt,
            step_min=step_min,
            business_tz_name=business_tz_name,
            overlap_delta=overlap_delta,
            publish_every_slices=publish_every_slices,
            publish_only_if_new=publish_only_if_new,
            publish_min_new_rows=publish_min_new_rows,
            always_publish_at_end=always_publish_at_end,
            backfill_missing_enabled=backfill_missing_enabled,
            phx_table=phx_table,
            phx_ts_col=phx_ts_col,
            phx_cols=phx_cols,
        )
        total_read, total_written_ch = _execute_with_lock(params)
        log.info("Готово. Прочитано: %d | в CH записано: %d", total_read, total_written_ch)

    finally:
        _close_quietly(phx, ch, pg)

def _run_etl(args: argparse.Namespace) -> None:
    """Тонкая оболочка: делегирует выполнение в `_run_etl_impl` и сразу выходит."""
    _run_etl_impl(args)

# --------------------------------- CLI ----------------------------------------

def main():
    """
    Тонкая оболочка: только парсинг аргументов и делегирование в `_run_etl`.
    """
    parser = argparse.ArgumentParser(description="Codes History Incremental (Phoenix→ClickHouse)")
    parser.add_argument("--since", required=False, help="ISO. Наивные значения трактуются в BUSINESS_TZ (если задана), иначе в UTC. Пр: 2025-08-08T00:00:00")
    parser.add_argument("--until", required=False, help="ISO. Наивные значения трактуются в BUSINESS_TZ (если задана), иначе в UTC. Если не указан — используется since + RUN_WINDOW_HOURS (по умолчанию 24 ч). Пр: 2025-08-09T00:00:00")
    parser.add_argument("--manual-start", action="store_true", help="Если указан, журнал ведётся под именем 'manual_<PROCESS_NAME>' (ручной запуск).")
    parser.add_argument("--log-level", "--log", dest="log_level", default="INFO", help="Уровень логирования (alias: --log)")
    parser.add_argument("--profile", action="store_true", help="Включить лёгкий профилировщик горячих участков (эквивалент ETL_PROFILE=1).")
    args = parser.parse_args()
    _run_etl(args)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _log = logging.getLogger("codes_history_increment")
        msg = "Остановлено пользователем (Ctrl+C)." if (_shutdown_reason in ("", "SIGINT")) else f"Остановлено сигналом {_shutdown_reason}."
        _log.warning("%s Выход с кодом 130.", msg)
        raise SystemExit(130)
    except PGConnectionError as pgerr:
        # Тихая обработка отказа Postgres на верхнем уровне.
        _log = logging.getLogger("codes_history_increment")
        _log.critical(
            "FATAL: postgres connect/init failed: %s — возможно PostgreSQL недоступен или указан некорректный DSN",
            pgerr,
        )
        raise SystemExit(2)
    except Exception as unhandled:
        # На этом этапе cfg может быть ещё не создан — ориентируемся на ENV/уровень логгера
        want_trace = _want_trace(None)
        _log = logging.getLogger("codes_history_increment")
        _msg = f"Необработанная ошибка: {unhandled.__class__.__name__}: {unhandled}"
        if want_trace:
            _log.exception(_msg)
        else:
            _log.error(_msg + " (стек скрыт; установите ETL_TRACE_EXC=1 или запустите с --log-level=DEBUG, чтобы напечатать трейсбек)")
        raise SystemExit(1)