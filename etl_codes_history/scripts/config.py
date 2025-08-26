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
from typing import List, Optional as _Optional

_log = _logging.getLogger(__name__)

def _candidate_env_paths() -> List[_pathlib.Path]:
    """Возвращает список кандидатов на расположение .env в порядке приоритета."""
    candidates: List[_pathlib.Path] = []
    env_file = (_os.getenv("ENV_FILE", "") or "").strip()
    if env_file:
        candidates.append(_pathlib.Path(env_file))
    candidates.append(_pathlib.Path.cwd() / ".env")
    try:
        this_file = _pathlib.Path(__file__).resolve()
        scripts_dir = this_file.parent
        repo_root = scripts_dir.parent
        candidates.append(repo_root / ".env")
        candidates.append(scripts_dir / ".env")
    except Exception:
        # Ничего страшного: просто пропустим корневые пути, если не удалось вычислить
        pass
    return candidates

def _load_env_try_dotenv(paths: List[_pathlib.Path]) -> _Optional[str]:
    """Пробует загрузить .env через python-dotenv. Возвращает путь, если успешно."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return None
    for p in paths:
        try:
            if p.is_file() and load_dotenv(dotenv_path=str(p), override=False):
                return str(p)
        except Exception:
            # Не останавливаемся на частной ошибке — пробуем следующий кандидат
            continue
    return None

def _load_env_try_fallback(paths: List[_pathlib.Path]) -> _Optional[str]:
    """Простой парсер KEY=VALUE на случай отсутствия python-dotenv."""
    for p in paths:
        try:
            if not p.is_file():
                continue
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    _os.environ.setdefault(k, v)
            return str(p)
        except Exception:
            continue
    return None

def _load_env_robust() -> str:
    candidates = _candidate_env_paths()
    loaded_from = _load_env_try_dotenv(candidates) or _load_env_try_fallback(candidates) or ""
    if loaded_from:
        _log.info("config: .env loaded from %s", loaded_from)
    else:
        _log.warning(
            "config: .env not found via ENV_FILE/CWD/repo_root/scripts — using process env only"
        )
    return loaded_from

_loaded_env_path = _load_env_robust()

def _ensure_pg_dsn_in_env():
    """
    Если PG_DSN не задан, собираем его из составных частей.
    Пароль по умолчанию не вшиваем в код (безопасность) — задаётся только через .env/окружение.
    """
    if "PG_DSN" in _os.environ and _os.environ["PG_DSN"].strip():
        return
    host = _os.getenv("PG_HOST", "127.0.0.1").strip()
    port = _os.getenv("PG_PORT", "5432").strip()
    db   = _os.getenv("PG_DB",   "etl_database").strip()
    user = _os.getenv("PG_USER", "etl_user").strip()
    # Безопасный дефолт: пустой пароль. Для локалки выставляется в .env.
    pwd  = _os.getenv("PG_PASSWORD", "").strip()
    _os.environ["PG_DSN"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    _log.info("config: PG_DSN synthesized from parts (PG_HOST/PORT/DB/USER/PASSWORD)")

_ensure_pg_dsn_in_env()
# --- END robust .env loader (injected) ---

from dataclasses import dataclass

def _as_bool(v: str, default: bool = False) -> bool:
    s = (v or "").strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default

def _env_int(name: str, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    """
    Безопасное чтение целого из окружения с опциональным ограничением снизу/сверху.
    - Пустые и некорректные значения заменяются на default.
    - Если указаны границы, значение зажимается в [min_value, max_value].
    """
    try:
        v = int(os.getenv(name, str(default)) or default)
    except Exception:
        v = default
    if min_value is not None and v < min_value:
        v = min_value
    if max_value is not None and v > max_value:
        v = max_value
    return v

@dataclass(frozen=True)
class Settings:
    # --- Identity & logging ---
    PROCESS_NAME: str = os.getenv("PROCESS_NAME", "codes_history_increment")
    ETL_TRACE_EXC: bool = _as_bool(os.getenv("ETL_TRACE_EXC", "0"))

    # --- Time semantics ---
    # If non-empty -> naive CLI dates are interpreted in this TZ; else -> UTC.
    BUSINESS_TZ: str = (os.getenv("BUSINESS_TZ", "") or "").strip()

    # --- PostgreSQL / Journal ---
    PG_DSN: str = os.getenv("PG_DSN", "")  # значение формируется _ensure_pg_dsn_in_env() или задаётся через окружение
    JOURNAL_TABLE: str = os.getenv("JOURNAL_TABLE", "public.inc_processing")
    JOURNAL_RETENTION_DAYS: int = _env_int("JOURNAL_RETENTION_DAYS", 30, min_value=1)
    JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC: int = _env_int("JOURNAL_HEARTBEAT_MIN_INTERVAL_SEC", 300, min_value=1)
    JOURNAL_AUTOPRUNE_ON_DONE: bool = _as_bool(os.getenv("JOURNAL_AUTOPRUNE_ON_DONE", "1"), True)

    # --- Phoenix source ---
    PQS_URL: str = os.getenv("PQS_URL", "http://127.0.0.1:8765")
    HBASE_MAIN_TABLE: str = os.getenv("HBASE_MAIN_TABLE", "TBL_JTI_TRACE_CIS_HISTORY")
    HBASE_MAIN_TS_COLUMN: str = os.getenv("HBASE_MAIN_TS_COLUMN", "tm")
    # Единственный источник правды по размеру кадров; совпадает с PhoenixConfig (дефолт 20000).
    PHX_FETCHMANY_SIZE: int = _env_int("PHX_FETCHMANY", _env_int("PHX_FETCHMANY_SIZE", 20000, min_value=1), min_value=1)
    PHX_QUERY_OVERLAP_MINUTES: int = _env_int("PHX_QUERY_OVERLAP_MINUTES", 5, min_value=0)

    # --- ClickHouse sink ---
    CH_HOSTS: str = os.getenv("CH_HOSTS", "127.0.0.1")
    CH_PORT: int = _env_int("CH_PORT", 9000, min_value=1, max_value=65535)
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
    CH_INSERT_BATCH: int = _env_int("CH_INSERT_BATCH", 10000, min_value=1)

    # --- ETL cadence / gating ---
    # Окно по умолчанию для автозаполнения --until, когда флаг не указан.
    RUN_WINDOW_HOURS: int = _env_int("RUN_WINDOW_HOURS", 24, min_value=1)
    STEP_MIN: int = _env_int("STEP_MIN", 10, min_value=1)
    PUBLISH_EVERY_SLICES: int = _env_int("PUBLISH_EVERY_SLICES", 0, min_value=0)
    PUBLISH_ONLY_IF_NEW: int = int(os.getenv("PUBLISH_ONLY_IF_NEW", "1"))
    PUBLISH_MIN_NEW_ROWS: int = _env_int("PUBLISH_MIN_NEW_ROWS", 1, min_value=0)

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
    
# -------------------- Phoenix client config (centralized) --------------------
# Этот блок даёт единый источник правды для настроек клиента Phoenix.
# Всё читается из переменных окружения (.env) и доступно из кода через
# get_phoenix_config(). Здесь также обеспечена обратная совместимость по именам
# переменных окружения (PHX_FETCHMANY vs PHX_FETCHMANY_SIZE).

from dataclasses import dataclass as _dataclass
import os as _os

@_dataclass(frozen=True)
class PhoenixConfig:
    # Базовый URL Avatica (Phoenix Query Server)
    PQS_URL: str
    # Быстрый TCP-probe (мс) перед подключением (0 — выключено)
    PHX_TCP_PROBE_TIMEOUT_MS: int
    # Адаптивное деление временных окон
    PHX_OVERLOAD_MIN_SPLIT_MIN: int
    PHX_OVERLOAD_MAX_DEPTH: int
    PHX_OVERLOAD_RETRY_ATTEMPTS: int
    PHX_OVERLOAD_RETRY_BASE_MS: int
    # Управление ORDER BY (True => отключить ORDER BY в запросе)
    PHX_DISABLE_ORDER_BY: bool
    # Фиксированный «начальный» размер слайса (минуты), 0 — выключено
    PHX_INITIAL_SLICE_MIN: int
    # Размер кадров для cursor.fetchmany()
    PHX_FETCHMANY: int

def get_phoenix_config() -> PhoenixConfig:
    """
    Читает переменные окружения и возвращает иммутабельный конфиг Phoenix.

    Обратная совместимость:
    - Если задана PHX_FETCHMANY — используем её.
    - Иначе, если задана устаревшая PHX_FETCHMANY_SIZE — используем её.
    - В противном случае — дефолт 20000.
    """
    pqs_url = (_os.getenv("PQS_URL", "") or "").strip()
    if not pqs_url:
        raise ValueError("PQS_URL is required (e.g., http://host:8765)")

    # PHX_FETCHMANY: приоритет новой переменной, затем — старой
    fetchmany_env = _os.getenv("PHX_FETCHMANY")
    if fetchmany_env is None:
        fetchmany = _env_int("PHX_FETCHMANY_SIZE", 20000, min_value=1)
    else:
        fetchmany = _env_int("PHX_FETCHMANY", 20000, min_value=1)

    return PhoenixConfig(
        PQS_URL=pqs_url,
        PHX_TCP_PROBE_TIMEOUT_MS=_env_int("PHX_TCP_PROBE_TIMEOUT_MS", 0, min_value=0),
        PHX_OVERLOAD_MIN_SPLIT_MIN=_env_int("PHX_OVERLOAD_MIN_SPLIT_MIN", 2, min_value=1),
        PHX_OVERLOAD_MAX_DEPTH=_env_int("PHX_OVERLOAD_MAX_DEPTH", 3, min_value=0),
        PHX_OVERLOAD_RETRY_ATTEMPTS=_env_int("PHX_OVERLOAD_RETRY_ATTEMPTS", 3, min_value=0),
        PHX_OVERLOAD_RETRY_BASE_MS=_env_int("PHX_OVERLOAD_RETRY_BASE_MS", 700, min_value=0),
        PHX_DISABLE_ORDER_BY=_as_bool(_os.getenv("PHX_DISABLE_ORDER_BY", "0")),
        # По умолчанию фиксированный «начальный» срез 5 минут, как договорились.
        PHX_INITIAL_SLICE_MIN=_env_int("PHX_INITIAL_SLICE_MIN", 5, min_value=0),
        PHX_FETCHMANY=fetchmany,
    )
__all__ = ["Settings", "PhoenixConfig", "get_phoenix_config"]