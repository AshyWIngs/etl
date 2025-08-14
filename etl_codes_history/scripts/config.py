# file: scripts/config.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
from datetime import datetime, timezone

class Settings(BaseSettings):
    """
    Конфиг загружается из .env (см. .env.example).
    """
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # Phoenix
    PQS_URL: str
    PHX_TS_UNITS: str = "timestamp"   # 'seconds' | 'millis' | 'timestamp'
    PHX_FETCHMANY_SIZE: int = 5000

    # Источник данных
    HBASE_MAIN_TABLE: str = "TBL_JTI_TRACE_CIS_HISTORY"
    HBASE_MAIN_TS_COLUMN: str = "opd"
    HBASE_MAIN_COLUMNS: str = (
        "c,t,opd,id,did,rid,rinn,rn,sid,sinn,sn,gt,prid,st,ste,elr,emd,apd,exd,"
        "p,pt,o,pn,b,tt,tm,ch,j,pg,et,pvad,ag"
    )

    # PostgreSQL журнал
    PG_DSN: str
    JOURNAL_TABLE: str = "public.inc_processing"
    PROCESS_NAME: str = "codes_history_increment"
    JOURNAL_RETENTION_DAYS: int = 0

    # ETL шаг/поведение
    STEP_MIN: int = 60
    PHX_QUERY_SHIFT_MINUTES: int = -300

    # ClickHouse — единственный sink; параметр SINK удалён.
    CH_HOSTS: str = "127.0.0.1"
    CH_PORT: int = 9000
    CH_DB: str = "stg"
    CH_USER: str = "default"
    CH_PASSWORD: str = ""
    # Таблицы ClickHouse (stg-схема)
    CH_RAW_TABLE: str = "stg.daily_codes_history_raw_all"
    CH_CLEAN_TABLE: str = "stg.daily_codes_history"
    CH_CLEAN_ALL_TABLE: str = "stg.daily_codes_history_all"
    CH_DEDUP_BUF_TABLE: str = "stg.daily_codes_history_dedup_buf"
    CH_INSERT_BATCH: int = 20000
    CH_INSERT_MAX_RETRIES: int = 1  # сколько раз повторяем insert при сбое соединения

    def main_columns_list(self) -> List[str]:
        return [c.strip() for c in (self.HBASE_MAIN_COLUMNS or "").split(",") if c.strip()]

    def ch_hosts_list(self) -> List[str]:
        return [h.strip() for h in (self.CH_HOSTS or "").split(",") if h.strip()]

def parse_iso_utc(s: str) -> datetime:
    """
    ISO строка → aware UTC datetime.
    'Z' поддерживается. Если без TZ — считаем это UTC.
    """
    s = (s or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)