# ETL: Codes History Increment (Phoenix/HBase → CSV + PG Journal)

Инкрементальная выгрузка историй кодов из Phoenix (HBase) в CSV с журналированием запусков и хранением `watermark` в PostgreSQL.

---

## Коротко о ключевых фичах

- **Эксклюзивный запуск** процесса по `process_name` через PG advisory-lock (`pg_try_advisory_lock(hashtext(...))`).
- **Один активный запуск** на процесс: частичный **UNIQUE** индекс на `planned|running` (без `ts_end`).
- **Heartbeat**: регулярный пульс в `details.heartbeat_ts` + прогресс.
- **Санация «висячих» запусков**: `sanitize_stale()`:
  - `planned` старше TTL → `skipped`
  - `running` с протухшим heartbeat или сильно старый → `error`
- **Watermark**: UPSERT с `GREATEST()` (не даёт откатить прогресс назад).
- **Миграции** вшиты в код (можно прогнать `--migrate-only`) + автосоздание DDL в `ensure()`.

---

## Состав репо (важные файлы)

- `scripts/codes_history_etl.py` — основной ETL.
- `scripts/journal.py` — журнал + watermark + миграции + эксклюзивная блокировка.
- `scripts/db/pg_client.py` — PG-клиент.
- `scripts/db/phoenix_client.py` — Phoenix PQS клиент.
- `scripts/config.py` — конфиг/ENV.
- `.env` / `.env.example` — настройки окружения.

---

## Требования

- Python ≥ 3.9 (используем `zoneinfo`; у вас Python 3.13 — ок).
- Доступ к Phoenix PQS и PostgreSQL.
- Директория для выгрузок CSV (из `.env`).

---

## Настройки (ENV)

Создайте `.env` по образцу `.env.example`:

```ini
# PostgreSQL
PG_DSN=postgresql://user:pass@host:5432/dbname
JOURNAL_TABLE=public.inc_processing
PROCESS_NAME=codes_history_increment

# Phoenix
PQS_URL=http://10.254.3.112:8765
PHX_FETCHMANY_SIZE=5000
PHX_TS_UNITS=timestamp   # или 'millis' если TS в миллисекундах

# Источник данных
HBASE_MAIN_TABLE=TBL_JTI_TRACE_CIS_HISTORY
HBASE_MAIN_TS_COLUMN=opd

# Слайс по умолчанию
STEP_MIN=60

# Каталог экспорта
EXPORT_DIR=./export
EXPORT_PREFIX=codes_history_

# Бизнесс-часовой пояс (дни считаем по КЗ)
BUSINESS_TZ=Asia/Almaty  # UTC+5

---

## Быстрый старт
python3 -m venv etl-env
source etl-env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 1) Подготовьте окружение
cp .env.example .env             # заполните значения
python -m scripts.codes_history_etl --migrate-only  # применить встроенные миграции в PG

# 2) Пробный прогон (DRY-RUN), сутки Казахстана 2025-08-01 (UTC+5)
python -m scripts.codes_history_etl \
  --since "2025-08-01T00:00:00+05:00" \
  --until "2025-08-02T00:00:00+05:00" \
  --dry-run

# 3) Боевой прогон (CSV будет записан)
python -m scripts.codes_history_etl \
  --since "2025-08-01T00:00:00+05:00" \
  --until "2025-08-02T00:00:00+05:00"

---

## CLI и сценарии запуска

# Только миграции (DDL) — создаст таблицы/индексы, безопасно повторно
python -m scripts.codes_history_etl --migrate-only

# Простой часовой срез в UTC
python -m scripts.codes_history_etl \
  --since "2025-08-01T11:00:00Z" \
  --until "2025-08-01T12:00:00Z"

# Ровные сутки Казахстана (UTC+5)
python -m scripts.codes_history_etl \
  --since "2025-08-01T00:00:00+05:00" \
  --until "2025-08-02T00:00:00+05:00"

# DRY-RUN (ничего не пишет на диск)
python -m scripts.codes_history_etl ... --dry-run

## ClickHouse

#Миграция
```sql
-- =========================
-- STG: локальная таблица на шарде
-- =========================
DROP TABLE IF EXISTS stg.daily_codes_history ON CLUSTER shardless SYNC;

CREATE TABLE stg.daily_codes_history ON CLUSTER shardless
(
    -- Обязательные поля (NOT NULL)
    c   String,           -- cis_id
    t   UInt8,            -- тип операции (0,1,2,...)
    opd DateTime,         -- дата/время операции (как в источнике)

    -- Остальные поля допускают NULL (по спецификации многие могут быть пустыми)
    id   Nullable(String),    -- md5(did) (перезаписывается в ETL); если did пустой -> NULL
    did  Nullable(String),

    rid  Nullable(String),  rinn Nullable(String),  rn  Nullable(String),
    sid  Nullable(String),  sinn Nullable(String),  sn  Nullable(String),

    gt   Nullable(String),
    prid Nullable(String),

    st   Nullable(UInt8),
    ste  Nullable(UInt8),
    elr  Nullable(UInt8),

    emd  Nullable(DateTime),
    apd  Nullable(DateTime),
    exd  Nullable(DateTime),

    p    Nullable(String),
    pt   Nullable(UInt8),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),

    tt   Nullable(Int64),
    tm   Nullable(DateTime),

    -- ВАЖНО: в Phoenix это VARCHAR ARRAY → в CH храним как массив строк
    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),
    j    Nullable(String),

    pg   Nullable(UInt16),
    et   Nullable(UInt8),

    pvad Nullable(String),
    ag   Nullable(String),

    -- системные/служебные поля от ETL
    ts DateTime,                  -- верх бизнес-окна (UTC), задаётся ETL
    q  UInt8 DEFAULT 0,           -- флаг дублей/служебный

    -- Денормы по ts для партиций/аналитики (Asia/Almaty)
    ts_biz DateTime MATERIALIZED toTimeZone(ts, 'Asia/Almaty'),
    d_biz  Date     MATERIALIZED toDate(ts_biz),
    h_biz  UInt8    MATERIALIZED toHour(ts_biz),

    -- Метаданные загрузки
    ts_ingested DateTime DEFAULT now(),
    etl_job LowCardinality(String) DEFAULT 'codes_history_etl',
    load_id UUID DEFAULT generateUUIDv4()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history', '{shardless_repl}')
PARTITION BY toYYYYMMDD(d_biz)
-- Ключ сортировки должен быть non-nullable: заворачиваем Nullable поля в ifNull()
ORDER BY (c, opd, ifNull(et, toUInt8(0)), ifNull(st, toUInt8(0)), ifNull(ste, toUInt8(0)))
SETTINGS index_granularity = 8192;

-- =========================
-- Распределённая таблица
-- =========================
DROP TABLE IF EXISTS stg.daily_codes_history_all ON CLUSTER shardless SYNC;

CREATE TABLE stg.daily_codes_history_all ON CLUSTER shardless
AS stg.daily_codes_history
ENGINE = Distributed('shardless', 'stg', 'daily_codes_history', cityHash64(c));

-- (опционально) TTL на год хранения:
 ALTER TABLE stg.daily_codes_history ON CLUSTER shardless
   MODIFY TTL ts_biz + INTERVAL 365 DAY DELETE;
```