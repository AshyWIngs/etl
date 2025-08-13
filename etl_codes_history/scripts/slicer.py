# file: scripts/slicer.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Iterator, Tuple

def iter_slices(since: datetime, until: datetime, step_min: int) -> Iterator[Tuple[datetime, datetime]]:
    """
    Режет [since; until) на равные слайсы step_min.
    Левая граница включительно, правая — исключительная (ClickHouse/SQL friendly).
    """
    step = timedelta(minutes=int(step_min))
    cur = since
    while cur < until:
        nxt = min(cur + step, until)
        yield cur, nxt
        cur = nxt