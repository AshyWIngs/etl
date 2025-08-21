# -*- coding: utf-8 -*-
"""
Высокопроизводительный клиент Phoenix (Avatica/PQS) для ETL.

Ключевые цели этого клиента:
• Минимизировать накладные расходы на чтение больших окон по времени.
• Дать один «источник правды» для адаптивной нарезки окна и бэк-оффа при перегрузке JobManager.
• Не тянуть в конфиг лишние опции: работаем через PQS_URL (Avatica) и protobuf по умолчанию.
• Бережно относиться к Phoenix/PQS: уметь дробить окно и ретраиться без избыточного давления.

Что важно для перформанса:
• Генерируем batches (list[dict]) без лишних преобразований; маппинг колонок — через zip(names, row).
• Используем cursor.arraysize = fetchmany_size (если драйвер позволяет).
• Поддерживаем «начальный» размер среза (PHX_INITIAL_SLICE_MIN, по умолчанию 5 минут) с выравниванием по UTC-сетке.
  Если PHX_INITIAL_SLICE_MIN=0 — адаптивная нарезка отключена, окно читается одним куском.
• Бэк-офф и рекурсивный сплит только при типовых признаках перегрузки (RejectedExecutionException, JobManager queue rejected).

Зависимости:
• Требуется пакет `phoenixdb` (Avatica). Сериализация — protobuf (дефолтно, менять не даём — стабильно и быстро).
"""

from __future__ import annotations

import logging
import os
import random
import socket
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse

try:
    import phoenixdb  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - для среды разработки без установленного драйвера
    phoenixdb = None  # type: ignore[assignment]

log = logging.getLogger("scripts.db.phoenix_client")


# ------------------------ вспомогательные утилиты ------------------------

# --- Безопасное квотирование идентификаторов Phoenix -------------------------
def _quote_ident(name: str) -> str:
    """
    Phoenix/SQL идентификаторы:
    - Без кавычек Phoenix поднимает регистр до UPPERCASE.
    - Если колонки в схеме созданы в кавычках и в нижнем регистре ("opd", "c"),
      обращение без кавычек приведёт к ошибке Undefined column (ищется OPD/C).
    - Кавычим только "простые" нижние имена (буквы/цифры/подчёркивание) и только если
      имя не процитировано заранее. Это не трогает выражения вида COUNT(*), to_char(...), "*".
    Производительность: это строковые операции при сборке SQL (однократно на запрос) — влияние пренебрежимо мало.
    """
    n = (name or "").strip()
    if not n or n == "*" or (n.startswith('"') and n.endswith('"')):
        return n
    is_simple = all(ch.islower() or ch.isdigit() or ch == "_" for ch in n)
    if is_simple and n[0].isalpha():
        return f'"{n}"'
    return n

def _render_columns(columns) -> str:
    """Строим SELECT-список с безопасным квотированием; None/['*'] → '*'. Сохраняем выражения как есть."""
    if not columns:
        return "*"
    if len(columns) == 1 and columns[0] == "*":
        return "*"
    return ", ".join(_quote_ident(c) for c in columns)

def _env_int(name: str, default: int) -> int:
    """Безопасное чтение целого из ENV (пустые/невалидные → default)."""
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _is_overload_error(exc: BaseException) -> bool:
    """Эвристика перегрузки Phoenix JobManager/очереди (распознаём типовые сообщения)."""
    try:
        s = f"{exc}".lower()
    except Exception:
        return False
    return ("rejectedexecutionexception" in s) or ("jobmanager" in s and "rejected" in s)


def _iter_utc_grid(f: datetime, t: datetime, step_min: int) -> Iterator[Tuple[datetime, datetime]]:
    """
    Итератор «сеточных» UTC-срезов [s;e) с шагом step_min минут.
    Производительность: это лёгкая арифметика по времени и один генератор — влияние на горячий путь чтения отсутствует.
    """
    cur = f
    while cur < t:
        nxt = PhoenixClient._slice_grid_ceil_utc(cur, step_min)
        if nxt <= cur:
            # Защита от «нулевого» шага при совпадении на границе
            nxt = cur + timedelta(minutes=step_min)
        if nxt > t:
            nxt = t
        yield cur, nxt
        cur = nxt


def _should_split_window(s0: datetime, e0: datetime, min_split_min: int, depth: int, max_depth: int) -> bool:
    """
    Чистая эвристика: решаем, нужно ли делить окно (по длительности и ограничению глубины).
    Вынесено отдельно, чтобы снизить когнитивную сложность основного метода.
    """
    span_min = max(0, int((e0 - s0).total_seconds() // 60))
    return (span_min > min_split_min) and (depth < max_depth)



def _compute_backoff_seconds(attempt: int) -> float:
    """
    Экспоненциальный бэк-офф с небольшим джиттером.
    Вынесено отдельно, чтобы не загромождать основной цикл.
    """
    return (2.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)


# --- Адаптивные параметры из ENV через dataclass ---
@dataclass(frozen=True)
class _AdaptiveParams:
    """Набор параметров адаптивной вытяжки из ENV.
    Выделено в dataclass, чтобы упростить код вызова и снизить когнитивную сложность.
    """
    init_min: int        # PHX_INITIAL_SLICE_MIN
    max_attempts: int    # PHX_OVERLOAD_MAX_ATTEMPTS
    min_split_min: int   # PHX_OVERLOAD_MIN_SPLIT_MIN
    max_depth: int       # PHX_OVERLOAD_MAX_DEPTH

def _load_adaptive_params_from_env() -> _AdaptiveParams:
    """Читает ENV и возвращает параметры адаптивной вытяжки с безопасными дефолтами.
    Вынесено отдельно — это упрощает основной метод и облегчает тестирование.
    """
    return _AdaptiveParams(
        init_min=_env_int("PHX_INITIAL_SLICE_MIN", 5),        # 5 мин по умолчанию (включено)
        max_attempts=_env_int("PHX_OVERLOAD_MAX_ATTEMPTS", 4),
        min_split_min=_env_int("PHX_OVERLOAD_MIN_SPLIT_MIN", 2),
        max_depth=_env_int("PHX_OVERLOAD_MAX_DEPTH", 3),
    )


# ------------------------ основной клиент ------------------------

class PhoenixClient:
    """
    Лёгкий клиент поверх phoenixdb:
    - ленивое соединение (коннектимcя при первом запросе);
    - чтение по окну времени (>= from_dt, < to_dt) упорядочено по ts-колонке;
    - адаптивная нарезка + бэк-офф при перегрузке (fetch_increment_adaptive).
    """

    def __init__(self, url: str, *, fetchmany_size: int = 1000) -> None:
        """
        :param url: PQS/Avatica URL (напр. http://10.254.3.112:8765)
        :param fetchmany_size: размер fetchmany для курсора (горячий параметр)
        """
        self._url = str(url)
        self._fetchmany_size = int(fetchmany_size or 1000)
        self._conn = None  # ленивый коннект

        # ENV-параметры (строго минимальный набор):
        # - PHX_TCP_PROBE_TIMEOUT_MS — опциональная TCP-проверка доступности Avatica до HTTP, чтобы быстро отвалиться
        # - PHX_INITIAL_SLICE_MIN — «начальный» размер среза для адаптивной нарезки (минуты); 0 → полностью отключить
        # - PHX_OVERLOAD_MAX_ATTEMPTS — число повторов при перегрузке очереди JobManager
        # - PHX_OVERLOAD_MIN_SPLIT_MIN — нижняя граница размера под-окна (минуты) при рекурсивном сплите
        # - PHX_OVERLOAD_MAX_DEPTH — максимальная глубина рекурсивного сплита
        self._tcp_probe_timeout_ms = _env_int("PHX_TCP_PROBE_TIMEOUT_MS", 0)

        # Кэш проекций (имена колонок) — снижает накладные расходы на `description`
        self._last_projection_key: Optional[Tuple[str, ...]] = None
        self._last_projection_names: Optional[Tuple[str, ...]] = None

    # ------------------------ жизненный цикл соединения ------------------------

    def close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()  # type: ignore[call-arg]
        except Exception:
            pass
        finally:
            self._conn = None

    def _tcp_probe(self) -> None:
        """Опциональная быстрая TCP-проверка доступности Avatica (до HTTP запроса).
        Управляется PHX_TCP_PROBE_TIMEOUT_MS (мс). 0/пусто — отключено.
        """
        timeout_ms = int(self._tcp_probe_timeout_ms or 0)
        if timeout_ms <= 0:
            return
        try:
            u = urlparse(self._url)
            host = u.hostname or "localhost"
            port = u.port or 8765
            # Неблокирующая короткая попытка — если не открывается, сразу бросаем исключение
            with socket.create_connection((host, port), timeout=timeout_ms / 1000.0):
                return
        except Exception as e:
            raise ConnectionError(f"TCP probe to {self._url!r} failed in {timeout_ms} ms: {e}")

    def _ensure_conn(self):
        """Ленивый коннект; сериализация — строго protobuf (стабильнее и быстрее)."""
        if self._conn is not None:
            return self._conn
        if phoenixdb is None:
            raise RuntimeError("Драйвер 'phoenixdb' не установлен в окружении.")
        self._tcp_probe()  # быстрый фэйл раньше HTTP, если включено
        # autocommit=True — мы только читаем; это снижает накладные расходы
        self._conn = phoenixdb.connect(self._url, autocommit=True, serialization="protobuf")  # type: ignore[call-arg]
        return self._conn

    # ------------------------ вспомогательное ------------------------

    @staticmethod
    def _as_naive_utc(dt: datetime) -> datetime:
        """Phoenix ожидает naive UTC datetime (tzinfo=None). Аккуратно приводим."""
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    @staticmethod
    def _slice_grid_ceil_utc(dt: datetime, step_min: int) -> datetime:
        """Следующая «сеточная» граница (UTC) с шагом step_min минут, не раньше dt."""
        # dt считаем naive-UTC
        total_min = dt.hour * 60 + dt.minute
        ceil_total = ((total_min + step_min - 1) // step_min) * step_min
        day_in_min = 24 * 60
        days_add, mins_in_day = divmod(ceil_total, day_in_min)
        base = datetime(dt.year, dt.month, dt.day, mins_in_day // 60, mins_in_day % 60)
        return base + timedelta(days=days_add)

    # ------------------------ базовое оконное чтение ------------------------

    def fetch_increment(
        self,
        table: str,
        ts_col: str,
        columns: Sequence[str],
        from_dt: datetime,
        to_dt: datetime,
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Чтение данных из Phoenix за окно [from_dt; to_dt) одним запросом.
        Возвращает генератор батчей (list[dict]).
        """
        if not columns:
            raise ValueError("columns пуст")
        if to_dt <= from_dt:
            return
        conn = self._ensure_conn()
        f = self._as_naive_utc(from_dt)
        t = self._as_naive_utc(to_dt)

        # ВАЖНО: Phoenix по умолчанию поднимает регистр идентификаторов до UPPERCASE.
        # Чтобы не ловить ERROR 504 (Undefined column) на колонках, созданных в нижнем регистре,
        # аккуратно квотируем «простые» имена колонок и поле времени.
        cols_sql = _render_columns(columns)
        ts_sql = _quote_ident(ts_col)

        sql = (
            f"SELECT {cols_sql} "
            f"FROM {table} "
            f"WHERE {ts_sql} >= ? AND {ts_sql} < ? "
            f"ORDER BY {ts_sql} ASC"
        )

        cur = conn.cursor()
        try:
            try:
                # Если драйвер поддерживает — уменьшаем число round-trip'ов
                cur.arraysize = int(self._fetchmany_size)
            except Exception:
                pass

            cur.execute(sql, (f, t))

            # Быстрый путь для имён колонок (phoenixdb.cursor.description)
            key = tuple(columns)
            if self._last_projection_key == key and self._last_projection_names is not None:
                names = self._last_projection_names
            else:
                desc = getattr(cur, "description", None)
                names = tuple(d[0] for d in desc) if desc else key
                self._last_projection_key = key
                self._last_projection_names = names

            fetch_sz = int(self._fetchmany_size)
            while True:
                rows = cur.fetchmany(fetch_sz)
                if not rows:
                    break
                # Микроопт: локальные ссылки
                _n = names
                # Прямой zip → dict без лишних проверок
                batch = [dict(zip(_n, row)) for row in rows]
                yield batch
        finally:
            try:
                cur.close()
            except Exception:
                pass

    # ------------------------ хелперы логирования/ретраев ------------------------

    def _log_overload_split(self, s0: datetime, e0: datetime, span_min: int, depth: int, max_depth: int) -> None:
        """Отдельное логирование сплита окна — выделено для снижения когнитивной сложности."""
        log.warning(
            "Phoenix перегружен на окне %s → %s (≈%d мин). Делю окно (глубина %d/%d)...",
            s0.isoformat(), e0.isoformat(), span_min, depth, max_depth,
        )

    def _log_overload_retry(self, next_attempt: int, max_attempts: int, backoff: float) -> None:
        """Отдельное логирование ретрая — выделено для снижения когнитивной сложности."""
        log.warning(
            "Phoenix перегружен (JobManager queue). Повторю попытку %d/%d через %.1f с...",
            next_attempt, max_attempts, backoff,
        )

    def _pull_with_retries(
        self,
        table: str,
        ts_col: str,
        columns: Sequence[str],
        s0: datetime,
        e0: datetime,
        *,
        min_split_min: int,
        max_depth: int,
        max_attempts: int,
        depth: int = 0,
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Горячий путь: одна точка входа чтения под-окна [s0;e0).
        - Пытаемся читать.
        - На перегрузке: либо делим окно (если ещё можно), либо ждём и повторяем.
        - Иначе — пробрасываем исключение.
        Выделено из fetch_increment_adaptive для уменьшения когнитивной сложности верхнего метода.
        """
        attempt = 1
        while True:
            try:
                yield from self.fetch_increment(table, ts_col, columns, s0, e0)
                return
            except Exception as ex:
                if not _is_overload_error(ex):
                    # Не признаки перегрузки JobManager — отдать на уровень выше.
                    raise

                # Перегрузка: если можно — делим окно и читаем рекурсивно.
                if _should_split_window(s0, e0, min_split_min, depth, max_depth):
                    span_min = max(0, int((e0 - s0).total_seconds() // 60))
                    mid = s0 + (e0 - s0) / 2
                    self._log_overload_split(s0, e0, span_min, depth + 1, max_depth)
                    yield from self._pull_with_retries(
                        table, ts_col, columns, s0, mid,
                        min_split_min=min_split_min, max_depth=max_depth, max_attempts=max_attempts, depth=depth + 1,
                    )
                    yield from self._pull_with_retries(
                        table, ts_col, columns, mid, e0,
                        min_split_min=min_split_min, max_depth=max_depth, max_attempts=max_attempts, depth=depth + 1,
                    )
                    return

                # Иначе — ждём и повторяем, пока есть попытки.
                if attempt < max_attempts:
                    backoff = _compute_backoff_seconds(attempt)
                    self._log_overload_retry(attempt + 1, max_attempts, backoff)
                    time.sleep(backoff)
                    attempt += 1
                    continue

                # Попытки исчерпаны — пробрасываем.
                raise

    # ------------------------ адаптивная вытяжка ------------------------

    def _iter_slices_or_whole(self, f: datetime, t: datetime, init_min: int) -> Iterator[Tuple[datetime, datetime]]:
        """Единая точка выбора: либо одно окно целиком, либо сеточные слайсы.
        Нужна, чтобы убрать ветвление из fetch_increment_adaptive и снизить когнитивную сложность.
        Производительность: генератор очень лёгкий, на горячий путь не влияет.
        """
        if init_min <= 0:
            # Без нарезки — одно окно целиком
            yield f, t
            return
        # С нарезкой — ровно по UTC-сетке (00:00, 00:05, 00:10, ...)
        yield from _iter_utc_grid(f, t, init_min)

    def fetch_increment_adaptive(
        self,
        table: str,
        ts_col: str,
        columns: Sequence[str],
        from_dt: datetime,
        to_dt: datetime,
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Единая высокоуровневая вытяжка окна [from_dt; to_dt):
        • Читает параметры адаптивности из ENV один раз (dataclass `_AdaptiveParams`).
        • Аккуратно приводит границы к naive UTC (как требует Phoenix).
        • Итеративно проходит либо одно сплошное окно, либо UTC-слайсы (если включена нарезка),
          а на каждом отрезке применяет _pull_with_retries (бэк-офф/рекурсивный сплит при перегрузке).
        Производительность: вся тяжёлая работа в fetch_increment; здесь только дешёвая координация.
        """
        if to_dt <= from_dt:
            return

        # Параметры адаптивной логики (строго из ENV, централизованно):
        params = _load_adaptive_params_from_env()

        # Phoenix принимает naive UTC — приводим аккуратно и один раз.
        f = self._as_naive_utc(from_dt)
        t = self._as_naive_utc(to_dt)

        # Единый цикл поверх «одного окна» или «сеточных» срезов.
        for s, e in self._iter_slices_or_whole(f, t, params.init_min):
            yield from self._pull_with_retries(
                table, ts_col, columns, s, e,
                min_split_min=params.min_split_min,
                max_depth=params.max_depth,
                max_attempts=params.max_attempts,
                depth=0,
            )