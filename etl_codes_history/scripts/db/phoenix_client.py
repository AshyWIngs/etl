"""Клиент Phoenix (через PQS/Avatica). Корректно передаёт границы по TS."""
import logging
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Any
import phoenixdb
import phoenixdb.cursor
log = logging.getLogger("scripts.db.phoenix_client")
class PhoenixClient:
    def __init__(self, url: str, fetchmany_size: int = 5000, ts_units: str = "seconds"):
        self._url = url
        self._fetchmany_size = fetchmany_size
        self._ts_units = (ts_units or "seconds").lower().strip()
        self._conn = phoenixdb.connect(url, autocommit=True)
        self._cur = self._conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
        ps = getattr(phoenixdb, "paramstyle", None) or "qmark"
        log.info("Phoenix PQS подключен (%s), paramstyle=%s", url, ps)
    def close(self):
        try:
            self._cur.close()
        finally:
            self._conn.close()
    def _to_ts_param(self, dt: datetime) -> Any:
        """
        seconds  → int epoch seconds
        millis   → int epoch milliseconds
        timestamp→ java.sql.Timestamp (через phoenixdb.TimestampFromTicks)
        """
        # нормализуем к UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        if self._ts_units == "seconds":
            return int(dt.timestamp())
        if self._ts_units == "millis":
            return int(dt.timestamp() * 1000)

        # TIMESTAMP: отдаём совместимый объект для Avatica
        return phoenixdb.TimestampFromTicks(dt.timestamp())
    def fetch_increment(self, table: str, ts_column: str, columns: List[str],
                        start: datetime, end: datetime, fetchmany_size: int = None) -> Iterator[List[Dict]]:
        fetchmany_size = fetchmany_size or self._fetchmany_size
        cols_sql = "*" if columns == ["*"] else ", ".join(f'"{c}"' for c in columns)
        sql = "SELECT {cols} FROM \"{table}\" WHERE \"{ts}\" >= ? AND \"{ts}\" < ? ORDER BY \"{ts}\"".format(
            cols=cols_sql, table=table, ts=ts_column)
        start_param = self._to_ts_param(start)
        end_param = self._to_ts_param(end)
        log.info("Phoenix SQL: %s | params: [%s → %s] | ts_units=%s | fetchmany=%s",
                 sql, start, end, self._ts_units, fetchmany_size)
        self._cur.execute(sql, (start_param, end_param))
        while True:
            batch = self._cur.fetchmany(fetchmany_size)
            if not batch:
                break
            yield batch
    def max_ts_from_inc_processing(self, table: str = 'WORK.INC_PROCESSING', process_col: str = 'PROCESS',
                                   ts_col: str = 'TS', process_value: str = None) -> int | None:
        try:
            if process_value:
                sql = f'SELECT MAX("{ts_col}") AS mx FROM "{table}" WHERE "{process_col}" = ?'
                self._cur.execute(sql, (process_value,))
            else:
                sql = f'SELECT MAX("{ts_col}") AS mx FROM "{table}"'
                self._cur.execute(sql)
            row = self._cur.fetchone()
            return int(row["MX"]) if row and row.get("MX") is not None else None
        except Exception as e:
            log.warning("Не удалось прочитать MAX(%s) из %s: %s", ts_col, table, e)
            return None
