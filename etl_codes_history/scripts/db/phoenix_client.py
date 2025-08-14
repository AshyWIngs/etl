# file: scripts/db/phoenix_client.py
# -*- coding: utf-8 -*-
import logging
from typing import Dict, Iterator, List
from datetime import datetime
import phoenixdb

log = logging.getLogger("scripts.db.phoenix_client")

class PhoenixClient:
    """
    Подключается к Phoenix PQS и позволяет:
    - получить список колонок (LIMIT 0),
    - постранично читать инкремент за интервал [from; to).
    """

    def __init__(self, pqs_url: str, fetchmany_size: int = 5000, ts_units: str = "timestamp"):
        # Пробуем бинарный протокол Avatica (protobuf) — обычно быстрее JSON.
        serialization_used = "protobuf"
        try:
            # Новые версии phoenixdb поддерживают явный выбор protobuf
            self.conn = phoenixdb.connect(
                pqs_url,
                autocommit=True,
                serialization="protobuf",
            )
        except TypeError:
            # Старые версии phoenixdb не знают параметр `serialization` — используем дефолт (JSON)
            self.conn = phoenixdb.connect(
                pqs_url,
                autocommit=True,
            )
            serialization_used = "json"
        except Exception as e:
            # Любая другая ошибка при попытке protobuf — откатываемся на JSON по умолчанию
            log.warning("Phoenix protobuf connect failed (%s) — falling back to JSON.", e)
            self.conn = phoenixdb.connect(
                pqs_url,
                autocommit=True,
            )
            serialization_used = "json"
        self.cur = self.conn.cursor()
        self.fetchmany_size = int(fetchmany_size)
        self.ts_units = ts_units
        self.cur.arraysize = self.fetchmany_size  # подсказка драйверу: размер кадра/пакета
        log.info("Phoenix PQS подключен (%s), paramstyle=qmark, serialization=%s", pqs_url, serialization_used)

    def close(self):
        try:
            self.cur.close()
        finally:
            self.conn.close()

    def discover_columns(self, table: str) -> List[str]:
        """
        SELECT * LIMIT 0 → имена колонок как в описании таблицы.
        """
        self.cur.execute(f'SELECT * FROM "{table}" LIMIT 0')
        return [d[0] for d in (self.cur.description or [])]

    def fetch_increment(
        self,
        table: str,
        ts_col: str,
        columns: List[str],
        from_dt: datetime,
        to_dt: datetime,
    ) -> Iterator[List[Dict]]:
        """
        Читает блоками (fetchmany_size) интервал [from_dt, to_dt), упорядочено по ts_col.
        Возвращает список словарей {col: value}.
        from_dt/to_dt должны быть aware (UTC) — phoenixdb их корректно сериализует.
        """
        cols_quoted = ", ".join(f'"{c}"' for c in columns)
        sql = (
            f'SELECT {cols_quoted} FROM "{table}" '
            f'WHERE "{ts_col}" >= ? AND "{ts_col}" < ? '
            f'ORDER BY "{ts_col}"'
        )
        log.info(
            "Phoenix SQL: %s | params: [%s → %s] | ts_units=%s | fetchmany=%d",
            sql.replace("\n", " "),
            from_dt.isoformat(), to_dt.isoformat(),
            self.ts_units, self.fetchmany_size
        )
        self.cur.execute(sql, (from_dt, to_dt))

        while True:
            rows = self.cur.fetchmany(self.fetchmany_size)
            if not rows:
                break
            # описание курсора → имена колонок, порядок = как в SELECT
            names = [d[0] for d in (self.cur.description or [])]
            out = [dict(zip(names, r)) for r in rows]
            yield out
            