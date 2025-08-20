from datetime import datetime, timezone, timedelta
from scripts.db.phoenix_client import PhoenixClient

def dt(s):  # "2025-06-30T19:03:20Z" → aware UTC
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

class Dummy(PhoenixClient):
    def __init__(self, step_min):
        # не коннектимся к PQS в тесте
        self.conn = None
        self.cur = None
        self.fetchmany_size = 1
        self.ts_units = "timestamp"
        self._overload_min_split_sec = 120
        self._overload_max_depth = 3
        self._overload_retry_attempts = 0
        self._overload_retry_base_ms = 0
        self._order_by_enabled = True
        self._initial_slice_sec = step_min * 60

def collect_slices(step_min, a, b):
    c = Dummy(step_min)
    return list(c._iter_initial_slices(dt(a), dt(b)))

def test_disabled_returns_single_slice():
    sl = collect_slices(0, "2025-06-30T19:03:20Z", "2025-06-30T19:10:00Z")
    assert sl == [(dt("2025-06-30T19:03:20Z"), dt("2025-06-30T19:10:00Z"))]

def test_5min_aligns_utc_and_covers_exactly():
    sl = collect_slices(5, "2025-06-30T19:03:20Z", "2025-06-30T19:16:00Z")
    # первый короткий до ближайшей верхней границы 19:05, далее ровные шаги
    expect = [
        (dt("2025-06-30T19:03:20Z"), dt("2025-06-30T19:05:00Z")),
        (dt("2025-06-30T19:05:00Z"), dt("2025-06-30T19:10:00Z")),
        (dt("2025-06-30T19:10:00Z"), dt("2025-06-30T19:15:00Z")),
        (dt("2025-06-30T19:15:00Z"), dt("2025-06-30T19:16:00Z")),
    ]
    assert sl == expect

def test_exact_boundary_has_no_leading_short_slice():
    sl = collect_slices(5, "2025-06-30T19:05:00Z", "2025-06-30T19:15:00Z")
    assert sl == [
        (dt("2025-06-30T19:05:00Z"), dt("2025-06-30T19:10:00Z")),
        (dt("2025-06-30T19:10:00Z"), dt("2025-06-30T19:15:00Z")),
    ]

def test_microseconds_do_not_break_alignment():
    a = datetime(2025,6,30,19,4,59,999000,tzinfo=timezone.utc)
    b = datetime(2025,6,30,19,5,1, tzinfo=timezone.utc)
    c = Dummy(5)
    sl = list(c._iter_initial_slices(a,b))
    # первый – короткий до 19:05:00.000000
    assert sl[0][1] == datetime(2025,6,30,19,5,0,tzinfo=timezone.utc)
    # покрытие строго [a; b)
    assert sl[0][0] == a and sl[-1][1] == b
    # непересечение и непрерывность
    for (x1,y1),(x2,y2) in zip(sl, sl[1:]):
        assert y1 == x2 and x1 < y1 <= x2 < y2


# ===== Overload / retry policy tests (pure unit, without hitting PQS) =====

class RejectedExecutionException(Exception):
    """Local stub of Phoenix RejectedExecutionException for tests."""
    pass


class FlakyPQSSim:
    """
    Simulates PQS / Phoenix server behaviour: raises RejectedExecutionException
    when the requested slice is too 'fat' for the job queue, otherwise succeeds.
    """
    def __init__(self, reject_if_longer_or_equal_sec: int):
        self.reject_threshold = reject_if_longer_or_equal_sec
        self.calls = []  # list of (start, end)

    def fetch(self, start, end):
        self.calls.append((start, end))
        dur = (end - start).total_seconds()
        if dur >= self.reject_threshold:
            raise RejectedExecutionException(f"Too big slice: {dur}s")
        # would normally return rows; here we just signal success
        return True


def _consume_with_overload(client, start, end, pqs, depth=0):
    """
    Local, test-only implementation of overload handling to validate our policy.
    Uses client's _overload_min_split_sec and _overload_max_depth.
    On RejectedExecutionException it splits slice in half until small enough or depth cap.
    Returns a list of successfully processed sub-slices [(a,b), ...] with full coverage.
    """
    try:
        pqs.fetch(start, end)
        return [(start, end)]
    except RejectedExecutionException:
        # give up if уже мелко или достигли лимита глубины
        if ((end - start).total_seconds() <= getattr(client, "_overload_min_split_sec", 0)
                or depth >= getattr(client, "_overload_max_depth", 0)):
            raise
        mid = start + (end - start) / 2
        left = _consume_with_overload(client, start, mid, pqs, depth + 1)
        right = _consume_with_overload(client, mid, end, pqs, depth + 1)
        return left + right


def _assert_contiguous_cover(slices, a, b):
    assert slices, "no slices produced"
    assert slices[0][0] == a, "left boundary mismatch"
    assert slices[-1][1] == b, "right boundary mismatch"
    for (x1, y1), (x2, y2) in zip(slices, slices[1:]):
        assert y1 == x2, "slices are not contiguous"
        assert x1 < y1 <= x2 < y2, "slice ordering/overlap broken"


def test_overload_splits_5min_into_halves_until_ok():
    """
    Initial 5-min grid is too 'fat' for PQS (threshold=300s), so we expect binary split to 2.5-min.
    """
    a = dt("2025-06-30T19:00:00Z")
    b = dt("2025-06-30T19:10:00Z")

    c = Dummy(5)
    # allow splitting down to 60s, depth generous
    c._overload_min_split_sec = 60
    c._overload_max_depth = 6

    pqs = FlakyPQSSim(reject_if_longer_or_equal_sec=300)  # 5 minutes

    produced = []
    for s, e in c._iter_initial_slices(a, b):
        produced.extend(_consume_with_overload(c, s, e, pqs))

    # Two 5-min initial slices => each split once into two 2.5-min slices => total 4
    assert len(produced) == 4
    _assert_contiguous_cover(produced, a, b)

    # sanity: every successful slice is < 300s
    assert all((e - s).total_seconds() < 300 for s, e in produced)


def test_overload_gives_up_when_below_min_size():
    """
    If slice is already smaller/equal to min_split_sec, we re-raise on overload.
    """
    c = Dummy(5)
    c._overload_min_split_sec = 600  # 10 minutes
    c._overload_max_depth = 6
    pqs = FlakyPQSSim(reject_if_longer_or_equal_sec=300)

    s = dt("2025-06-30T19:00:00Z")
    e = dt("2025-06-30T19:05:00Z")  # exactly 300s -> PQS rejects,
    # but min_split_sec=600 prevents further splitting

    try:
        _consume_with_overload(c, s, e, pqs)
        assert False, "Expected RejectedExecutionException"
    except RejectedExecutionException:
        pass


def test_overload_gives_up_when_depth_cap_reached():
    """
    With a tight depth cap, repeated overloads should eventually bubble up.
    """
    c = Dummy(5)
    c._overload_min_split_sec = 1  # allow splitting very fine
    c._overload_max_depth = 1      # but only one split level allowed
    pqs = FlakyPQSSim(reject_if_longer_or_equal_sec=300)

    s = dt("2025-06-30T19:00:00Z")
    e = dt("2025-06-30T19:10:00Z")  # 600s -> reject, split once to 300s/300s -> still reject

    try:
        _consume_with_overload(c, s, e, pqs)
        assert False, "Expected RejectedExecutionException due to depth cap"
    except RejectedExecutionException:
        pass