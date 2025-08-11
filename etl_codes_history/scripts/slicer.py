from datetime import datetime, timedelta, timezone
from typing import Iterator, Tuple
def iter_slices(start: datetime, end: datetime, step_minutes: int) -> Iterator[Tuple[datetime, datetime]]:
    assert start.tzinfo is not None and end.tzinfo is not None, "Use aware datetimes (UTC)"
    step = timedelta(minutes=step_minutes)
    cur = start
    while cur < end:
        nxt = min(cur + step, end)
        yield cur, nxt
        cur = nxt
