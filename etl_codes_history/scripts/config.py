# -*- coding: utf-8 -*-
"""
Minimal Settings loader for ETL.

Этот модуль сознательно хранит только те переменные, которые реально используются
в текущем коде. У всех настроек есть безопасные дефолты, так что локальный запуск
работает «из коробки». Любую настройку можно переопределить через переменные окружения
(см. .env.example).

Ключевые решения:
- INPUT_TZ удалён. Если BUSINESS_TZ непустой, наивные даты CLI трактуются в этой TZ;
  иначе — как UTC. Это делает семантику времени однозначной.
- Ретенция журнала — только партициями; никаких row-level DELETE.
- Дефолты консервативные и безопасные для продакшена.
"""
from __future__ import annotations

import os
# --- BEGIN robust .env loader (injected) ---
# Загружаем переменные окружения из .env надёжно и предсказуемо.
# Порядок поиска:
#   1) ENV_FILE (если указан абсолютный или относительный путь)
#   2) CWD/.env
#   3) <repo_root>/.env   (родитель каталога, где лежит scripts/)
#   4) scripts/.env
# Плюс, если PG_DSN не задан, собираем его из PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD.
import logging as _logging, os as _os, pathlib as _pathlib

def _load_env_robust() -> str:
    candidates = []
    # 1) Явный путь через ENV_FILE
    env_file = _os.getenv("ENV_FILE", "").strip()
    if env_file:
        candidates.append(_pathlib.Path(env_file))
    # 2) Текущая рабочая директория
    candidates.append(_pathlib.Path.cwd() / ".env")
    # 3) Корень репозитория (родитель каталога scripts/)
    try:
        this_file = _pathlib.Path(__file__).resolve()
        scripts_dir = this_file.parent
        repo_root = scripts_dir.parent
        candidates.append(repo_root / ".env")
        # 4) Рядом с config.py (на случай локальных запусков из scripts/)
        candidates.append(scripts_dir / ".env")
    except Exception:
        pass

    loaded_from = ""
    # Пробуем через python-dotenv (если установлен)
    try:
        from dotenv import load_dotenv  # type: ignore
        for p in candidates:
            if p.is_file():
                if load_dotenv(dotenv_path=str(p), override=False):
                    loaded_from = str(p)
                    break
    except Exception:
        # Фолбэк: примитивный парсер KEY=VALUE (без кавычек/экранирования)
        for p in candidates:
            if p.is_file():
                try:
                    for line in p.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            k = k.strip()
                            v = v.strip().strip('"').strip("'")
                            _os.environ.setdefault(k, v)
                    loaded_from = str(p)
                    break
                except Exception:
                    continue

    if loaded_from:
        _logging.getLogger(__name__).info("config: .env loaded from %s", loaded_from)
    else:
        _logging.getLogger(__name__).warning(
            "config: .env not found via ENV_FILE/CWD/repo_root/scripts — using process env only"
        )
    return loaded_from

_loaded_env_path = _load_env_robust()

def _ensure_pg_dsn_in_env():
    """
    Если PG_DSN не задан, собираем его из составных частей.
    Это устраняет кейс, когда в .env задано PG_HOST/PORT/...,
    но нет единого PG_DSN.
    """
    if "PG_DSN" in _os.environ and _os.environ["PG_DSN"].strip():
        return
    host = _os.getenv("PG_HOST", "127.0.0.1").strip()
    port = _os.getenv("PG_PORT", "5432").strip()
    db   = _os.getenv("PG_DB",   "etl_database").strip()
    user = _os.getenv("PG_USER", "etl_user").strip()
    pwd  = _os.getenv("PG_PASSWORD", "etl_password").strip()
    _os.environ["PG_DSN"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    _logging.getLogger(__name__).info("config: PG_DSN synthesized from parts (PG_HOST/PORT/DB/USER/PASSWORD)")

_ensure_pg_dsn_in_env()
# --- END robust .env loader (injected) ---

from dataclasses import dataclass
from typing import List

def _as_bool(v: str, default: bool = False) -> bool:
    s = (v or "").strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default

@dataclass(frozen=True)
class Settings:
    # --- Identity & logging ---
    PROCESS_NAME: str = os.getenv("PROCESS_NAME", "codes_history_increment")
    ETL_TRACE_EXC: bool = _as_bool(os.getenv("ETL_TRACE_EXC", "0"))

    # --- Time semantics ---
    # If non-empty -> naive CLI dates are interpreted in this TZ; else -> UTC.
    BUSINESS_TZ: str = (os.getenv("BUSINESS_TZ", "") or "").strip()

    # --- PostgreSQL / Journal ---
    PG_DSN: str = os.getenv("PG_DSN", "postgresql://etl_user:etl_password@127.0.0.1:5432/etl_database")
    JOURNAL_TABLE: str = os.getenv("JOURNAL_TABLE", "public.inc_processing")
    JOURNAL_RETENTION_DAYS: int = int(os.getenv("JOURNAL_RETENTION_DAYS", "30"))
    JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC: int = int(os.getenv("JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", "300"))
    JOURNAL_AUTOPRUNE_ON_DONE: bool = _as_bool(os.getenv("JOURNAL_AUTOPRUNE_ON_DONE", "1"), True)

    # --- Phoenix source ---
    PQS_URL: str = os.getenv("PQS_URL", "http://127.0.0.1:8765")
    HBASE_MAIN_TABLE: str = os.getenv("HBASE_MAIN_TABLE", "TBL_JTI_TRACE_CIS_HISTORY")
    HBASE_MAIN_TS_COLUMN: str = os.getenv("HBASE_MAIN_TS_COLUMN", "opd")
    PHX_FETCHMANY_SIZE: int = int(os.getenv("PHX_FETCHMANY_SIZE", "20000"))
    PHX_TS_UNITS: str = os.getenv("PHX_TS_UNITS", "timestamp")  # "timestamp" or "millis"
    PHX_QUERY_OVERLAP_MINUTES: int = int(os.getenv("PHX_QUERY_OVERLAP_MINUTES", "5"))
    PHX_OVERLAP_ONLY_FIRST_SLICE: int = int(os.getenv("PHX_OVERLAP_ONLY_FIRST_SLICE", "1"))

    # --- ClickHouse sink ---
    CH_HOSTS: str = os.getenv("CH_HOSTS", "127.0.0.1")
    CH_PORT: int = int(os.getenv("CH_PORT", "9000"))
    CH_DB: str = os.getenv("CH_DB", "default")
    CH_USER: str = os.getenv("CH_USER", "default")
    CH_PASSWORD: str = os.getenv("CH_PASSWORD", "")
    CH_CLUSTER: str = (os.getenv("CH_CLUSTER", "") or "").strip()

    # RAW — Distributed; CLEAN — локальная (для REPLACE PARTITION);
    # CLEAN_ALL — Distributed-представление CLEAN.
    CH_RAW_TABLE: str = os.getenv("CH_RAW_TABLE", "stg.daily_codes_history_raw_all")
    CH_CLEAN_TABLE: str = os.getenv("CH_CLEAN_TABLE", "stg.daily_codes_history_all_local")
    CH_CLEAN_ALL_TABLE: str = os.getenv("CH_CLEAN_ALL_TABLE", "stg.daily_codes_history_all")
    CH_DEDUP_BUF_TABLE: str = os.getenv("CH_DEDUP_BUF_TABLE", "stg.daily_codes_history_buf")
    CH_INSERT_BATCH: int = int(os.getenv("CH_INSERT_BATCH", "10000"))

    # --- ETL cadence / gating ---
    STEP_MIN: int = int(os.getenv("STEP_MIN", "10"))
    PUBLISH_EVERY_SLICES: int = int(os.getenv("PUBLISH_EVERY_SLICES", "0"))
    PUBLISH_ONLY_IF_NEW: int = int(os.getenv("PUBLISH_ONLY_IF_NEW", "1"))
    PUBLISH_MIN_NEW_ROWS: int = int(os.getenv("PUBLISH_MIN_NEW_ROWS", "1"))

    # --- Columns fallback (при недоступности авто-дискавери через Phoenix) ---
    HBASE_MAIN_COLUMNS: str = os.getenv(
        "HBASE_MAIN_COLUMNS",
        "c,t,opd,id,did,rid,rinn,rn,sid,sinn,sn,gt,prid,st,ste,elr,emd,apd,exd,p,pt,o,pn,b,tt,tm,ch,j,pg,et,pvad,ag",
    )

    # -------- Helpers --------
    def ch_hosts_list(self) -> List[str]:
        return [h.strip() for h in self.CH_HOSTS.split(",") if h.strip()]

    def main_columns_list(self) -> List[str]:
        return [c.strip() for c in self.HBASE_MAIN_COLUMNS.split(",") if c.strip()]