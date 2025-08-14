# -*- coding: utf-8 -*-
# scripts/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

# Загружаем .env, если есть python-dotenv
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    _env = find_dotenv(usecwd=True)
    if _env:
        load_dotenv(_env, override=False)
except Exception:
    pass

def _clean(val: Optional[str]) -> str:
    if val is None:
        return ""
    s = val.strip()
    if not s:
        return ""
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s.strip("\"'")
    sharp = s.find("#")
    if sharp > 0:
        s = s[:sharp].strip()
    return s

def _get_str(name: str, default: str = "") -> str:
    return _clean(os.getenv(name, default))

def _get_int(name: str, default: int = 0) -> int:
    raw = _clean(os.getenv(name))
    if raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)

def _get_bool(name: str, default: bool = False) -> bool:
    raw = _clean(os.getenv(name))
    if raw == "":
        return bool(default)
    return raw.lower() in ("1", "true", "yes", "y", "on")

def _get_list(name: str, default: str = "", sep: str = ",") -> List[str]:
    raw = _clean(os.getenv(name, default))
    if raw == "":
        return []
    return [p.strip() for p in raw.split(sep) if p.strip()]

def parse_iso_utc(s: Optional[str]) -> datetime:
    if not s:
        raise ValueError("parse_iso_utc: empty input")
    ss = s.strip()
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"
    dt = datetime.fromisoformat(ss)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

@dataclass
class Settings:
    # Процесс и журнал
    PROCESS_NAME: str = field(default_factory=lambda: _get_str("PROCESS_NAME", "codes_history_increment"))
    JOURNAL_TABLE: str = field(default_factory=lambda: _get_str("JOURNAL_TABLE", "public.inc_processing"))
    JOURNAL_RETENTION_DAYS: int = field(default_factory=lambda: _get_int("JOURNAL_RETENTION_DAYS", 30))

    # PostgreSQL
    PG_DSN: str = field(default_factory=lambda: _get_str("PG_DSN", "postgresql://etl:etl@localhost:5432/etl"))

    # Phoenix / HBase
    PQS_URL: str = field(default_factory=lambda: _get_str("PQS_URL", "http://127.0.0.1:8765"))
    HBASE_MAIN_TABLE: str = field(default_factory=lambda: _get_str("HBASE_MAIN_TABLE", "TBL_JTI_TRACE_CIS_HISTORY"))
    HBASE_MAIN_TS_COLUMN: str = field(default_factory=lambda: _get_str("HBASE_MAIN_TS_COLUMN", "opd"))
    PHX_FETCHMANY_SIZE: int = field(default_factory=lambda: _get_int("PHX_FETCHMANY_SIZE", 5000))
    PHX_TS_UNITS: str = field(default_factory=lambda: _get_str("PHX_TS_UNITS", "timestamp"))  # 'timestamp'|'millis'|'micros'

    # Тайминги окна
    STEP_MIN: int = field(default_factory=lambda: _get_int("STEP_MIN", 10))
    PHX_QUERY_SHIFT_MINUTES: int = field(default_factory=lambda: _get_int("PHX_QUERY_SHIFT_MINUTES", 0))
    PHX_QUERY_LAG_MINUTES: int = field(default_factory=lambda: _get_int("PHX_QUERY_LAG_MINUTES", 0))
    PHX_QUERY_OVERLAP_MINUTES: int = field(default_factory=lambda: _get_int("PHX_QUERY_OVERLAP_MINUTES", 0))
    PHX_OVERLAP_ONLY_FIRST_SLICE: bool = field(default_factory=lambda: _get_bool("PHX_OVERLAP_ONLY_FIRST_SLICE", True))

    # ClickHouse
    CH_HOSTS: List[str] = field(default_factory=lambda: _get_list("CH_HOSTS", "127.0.0.1"))
    CH_PORT: int = field(default_factory=lambda: _get_int("CH_PORT", 9000))
    CH_DB: str = field(default_factory=lambda: _get_str("CH_DB", "stg"))
    CH_USER: str = field(default_factory=lambda: _get_str("CH_USER", "default"))
    CH_PASSWORD: str = field(default_factory=lambda: _get_str("CH_PASSWORD", ""))

    # RAW (Distributed) источник для дедупа должен указывать на *_raw_all
    # Поддерживаем совместимость: CH_RAW_TABLE (новое) или CH_TABLE (старое имя)
    CH_RAW_TABLE: str = field(default_factory=lambda: _get_str("CH_RAW_TABLE", _get_str("CH_TABLE", "stg.daily_codes_history_raw_all")))
    CH_CLEAN_TABLE: str = field(default_factory=lambda: _get_str("CH_CLEAN_TABLE", "stg.daily_codes_history"))
    CH_CLEAN_ALL_TABLE: str = field(default_factory=lambda: _get_str("CH_CLEAN_ALL_TABLE", "stg.daily_codes_history_all"))
    CH_DEDUP_BUF_TABLE: str = field(default_factory=lambda: _get_str("CH_DEDUP_BUF_TABLE", "stg.daily_codes_history_dedup_buf"))
    CH_INSERT_BATCH: int = field(default_factory=lambda: _get_int("CH_INSERT_BATCH", 20000))

    # Автокаденс публикаций
    PUBLISH_EVERY_MINUTES: int = field(default_factory=lambda: _get_int("PUBLISH_EVERY_MINUTES", 60))
    PUBLISH_EVERY_SLICES: int = field(default_factory=lambda: _get_int("PUBLISH_EVERY_SLICES", 6))
    ALWAYS_PUBLISH_AT_END: bool = field(default_factory=lambda: _get_bool("ALWAYS_PUBLISH_AT_END", True))

    # Поддерживаем переменные MAIN_COLUMNS (новая) и HBASE_MAIN_COLUMNS (устаревшая)
    MAIN_COLUMNS: List[str] = field(default_factory=lambda: _get_list(
            "MAIN_COLUMNS",
            _get_str("HBASE_MAIN_COLUMNS",
                     "c,t,opd,id,did,rid,rinn,rn,sid,sinn,sn,gt,prid,st,ste,elr,emd,apd,exd,p,pt,o,pn,b,tt,tm,ch,j,pg,et,pvad,ag")
        ))

    def main_columns_list(self) -> List[str]:
        return list(self.MAIN_COLUMNS)

    def ch_hosts_list(self) -> List[str]:
        return list(self.CH_HOSTS)