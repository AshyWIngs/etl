# file: scripts/slicer.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Iterator, Tuple

__all__ = ["iter_slices", "iter_slices_grid"]

def iter_slices(since: datetime, until: datetime, step_min: int) -> Iterator[Tuple[datetime, datetime]]:
    """
    Быстрый генератор равных полуинтервалов [since; until) длиной step_min минут.
    Оптимизации:
      • одна условная ветка на всю генерацию (без min() на каждом шаге);
      • избегаем плавающих расчётов (деление/остаток на timedelta);
      • защита от step_min <= 0 (возвращаем один слайс, чтобы не словить бесконечный цикл).

    Производительность: цикл O(n) с минимальными проверками; память O(1).
    """
    # Пустое окно — ничего не делаем (горячий быстрый путь)
    if since >= until:
        return

    # Нулевая/отрицательная длина шага: трактуем как «без нарезки» — один слайс целиком
    # (безопаснее для пайплайна, чем ValueError; логика отключения нарезки у нас встречается в конфиге).
    if step_min <= 0:
        yield since, until
        return

    step = timedelta(minutes=int(step_min))

    # Кол-во «полных» шагов и остаток считаем через целочисленную арифметику timedelta (без float)
    total = until - since
    full_steps = total // step           # type: ignore[operator]
    tail = total - full_steps * step     # type: ignore[operator]
    n = int(full_steps) + (1 if tail else 0)

    # Генерация: на каждой итерации одна операция сложения и одно присваивание
    cur = since
    for _ in range(n - 1):
        nxt = cur + step
        yield cur, nxt
        cur = nxt

    # Последний слайс — хвост до точной правой границы
    yield cur, until


def iter_slices_grid(since: datetime, until: datetime, step_min: int) -> Iterator[Tuple[datetime, datetime]]:
    """
    Быстрый генератор слайсов, привязанных к «сетке» по минутам внутри суток: 00:00, 00:step, 00:2*step, ...
    Это часто даёт прирост скорости на источниках (PQS/Phoenix/OLTP), т.к. одинаковые границы лучше прогревают кэши
    и распределяют нагрузку равномернее.

    Семантика не меняется: на выходе полуинтервалы в точности в пределах [since; until), сетка используется только
    как ориентир, а первый/последний слайсы подрезаются по фактическим границам.

    Примечания по производительности:
      • O(n) по количеству слайсов, O(1) по памяти;
      • только целочисленная арифметика с timedelta (никаких float);
      • быстрые выходы для пустого окна и step_min <= 0 (режим «без нарезки»).
    """
    if since >= until:
        return

    if step_min <= 0:
        # Режим без нарезки — один цельный слайс. Это и быстрее, и исключает риски бесконечного цикла.
        yield since, until
        return

    step_min = int(step_min)
    step = timedelta(minutes=step_min)

    # Нормализуем старт к минутной точности (секунды/микросекунды нам для сетки не нужны).
    # Важно: функция НЕ меняет таймзону — предполагается, что сюда уже пришли нужные (обычно UTC) даты.
    base = since.replace(second=0, microsecond=0)

    # Находим ближайшую "нижнюю" границу сетки: 00:00 + k * step, где k = floor(minutes_of_day / step_min)
    minutes_of_day = base.hour * 60 + base.minute
    shift = minutes_of_day % step_min
    grid_start = base - timedelta(minutes=shift)

    # Идём шагами по сетке, подрезая края под [since; until)
    cur = grid_start
    while cur < until:
        nxt = cur + step
        s = since if cur < since else cur
        e = until if nxt > until else nxt
        if s < e:
            yield s, e
        cur = nxt