from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, timezone
from typing import List

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # Phoenix
    PQS_URL: str
    PHX_TS_UNITS: str = "seconds"   # 'seconds' | 'millis' | 'timestamp'
    PHX_FETCHMANY_SIZE: int = 5000

    # Источник данных
    HBASE_MAIN_TABLE: str = "TBL_JTI_TRACE_CIS_HISTORY"
    HBASE_MAIN_TS_COLUMN: str = "opd"
    HBASE_MAIN_COLUMNS: str = (
        "c,t,opd,id,did,rid,rinn,rn,sid,sinn,sn,gt,prid,st,ste,elr,emd,apd,p,pt,o,"
        "pn,b,tt,tm,ch,j,pg,et,exd,pvad,ag"
    )

    # PostgreSQL журнал
    PG_DSN: str
    JOURNAL_TABLE: str = "public.inc_processing"
    PROCESS_NAME: str = "codes_history_increment"
    JOURNAL_RETENTION_DAYS: int = 0

    # ETL шаг/CSV отладка
    STEP_MIN: int = 60
    EXPORT_DIR: str = "exports"
    EXPORT_PREFIX: str = "codes_history_"

    # Системные поля
    ETL_ADD_SYS_FIELDS: int | bool = 1
    ETL_SYS_TS_NAME: str = "ts"
    ETL_SYS_ID_NAME: str = "id"

    # Таймзона бизнеса
    BUSINESS_TZ: str = "Asia/Almaty"

    # ClickHouse sink
    SINK: str = "clickhouse"  # clickhouse | csv | both
    CH_HOSTS: str = "127.0.0.1"
    CH_PORT: int = 9000
    CH_DB: str = "stg"
    CH_USER: str = "default"
    CH_PASSWORD: str = ""
    CH_TABLE: str = "stg.daily_codes_history_all"
    CH_INSERT_BATCH: int = 20000

    def main_columns_list(self) -> List[str]:
        return [c.strip() for c in (self.HBASE_MAIN_COLUMNS or "").split(",") if c.strip()]

    def ch_hosts_list(self) -> List[str]:
        return [h.strip() for h in (self.CH_HOSTS or "").split(",") if h.strip()]

def parse_iso_utc(s: str) -> datetime:
    """
    Принимает ISO-строку с возможной TZ (в т.ч. 'Z').
    Возвращает aware datetime в UTC.
    """
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)