-- =====================================================================
-- Константы через параметры ClickHouse (устраняем дубли литералов):
--   {tz_local:String} — локальная бизнес‑таймзона (по умолчанию 'Asia/Almaty')
--   {tz_utc:String}   — базовая таймзона хранения (по умолчанию 'UTC')
-- Пример запуска:
--   clickhouse-client --param_tz_local='Asia/Almaty' --param_tz_utc='UTC' \
--                     --param_repl_macro='{shardless_repl}' -n < stg_daily_codes_history.sql
-- Эти параметры используются только в типах/алиасах/аргументах движка и не влияют на производительность.
-- =====================================================================
-- RAW: приземление + TTL по времени загрузки
DROP TABLE IF EXISTS stg.daily_codes_history_raw ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history_raw ON CLUSTER shardless
(
    c   String,
    t   UInt8,
    opd DateTime64(3, {tz_utc:String}),
    opd_local DateTime64(3, {tz_local:String}) ALIAS toTimeZone(opd, {tz_local:String}),
    -- Удобная дата по Астане (для фильтров/джоинов/статистики по локальным суткам)
    opd_local_date Date MATERIALIZED toDate(toTimeZone(opd, {tz_local:String})),

    id   Nullable(String),
    did  Nullable(String),
    rid  Nullable(String),
    rinn Nullable(String),
    rn   Nullable(String),
    sid  Nullable(String),
    sinn Nullable(String),
    sn   Nullable(String),
    gt   Nullable(String),
    prid Nullable(String),
    st   Nullable(UInt8),
    ste  Nullable(UInt8),
    elr  Nullable(UInt8),
    emd  Nullable(DateTime64(3, {tz_utc:String})),
    emd_local Nullable(DateTime64(3, {tz_local:String})) ALIAS if(isNull(emd), NULL, toTimeZone(emd, {tz_local:String})),
    apd  Nullable(DateTime64(3, {tz_utc:String})),
    apd_local Nullable(DateTime64(3, {tz_local:String})) ALIAS if(isNull(apd), NULL, toTimeZone(apd, {tz_local:String})),
    exd  Nullable(DateTime64(3, {tz_utc:String})),
    exd_local Nullable(DateTime64(3, {tz_local:String})) ALIAS if(isNull(exd), NULL, toTimeZone(exd, {tz_local:String})),
    p    Nullable(String),
    pt   Nullable(UInt8),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),
    tt   Nullable(Int64),
    tm   Nullable(DateTime64(3, {tz_utc:String})),
    tm_local Nullable(DateTime64(3, {tz_local:String})) ALIAS if(isNull(tm), NULL, toTimeZone(tm, {tz_local:String})),
    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),
    j    Nullable(String) CODEC(ZSTD(6)),
    pg   Nullable(UInt16),
    et   Nullable(UInt8),
    pvad Nullable(String) CODEC(ZSTD(6)),
    ag   Nullable(String) CODEC(ZSTD(6)),
    -- Время приземления всегда в UTC для монотоности и одинаковых TTL на всех нодах,
    -- а для удобства добавлен человекочитаемый алиас в Asia/Almaty.
    ingested_at DateTime({tz_utc:String}) DEFAULT now({tz_utc:String}),
    ingested_at_local DateTime({tz_local:String}) ALIAS toTimeZone(ingested_at, {tz_local:String}),
    -- Скип-индекс по локальной дате для ускорения запросов WHERE opd_local_date = ...
    INDEX idx_opd_local_date (opd_local_date) TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history_raw', {repl_macro:String})
PARTITION BY toYYYYMMDD(opd)
ORDER BY (c, opd, t)
TTL ingested_at + INTERVAL 5 DAY DELETE
SETTINGS index_granularity = 8192;

-- Distributed над RAW
DROP TABLE IF EXISTS stg.daily_codes_history_raw_all ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history_raw_all ON CLUSTER shardless
AS stg.daily_codes_history_raw
ENGINE = Distributed('shardless', 'stg', 'daily_codes_history_raw', cityHash64(c));

-- Буфер для дедупа (без TTL!)
DROP TABLE IF EXISTS stg.daily_codes_history_dedup_buf ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history_dedup_buf ON CLUSTER shardless
AS stg.daily_codes_history_raw
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history_dedup_buf', {repl_macro:String})
PARTITION BY toYYYYMMDD(opd)
ORDER BY (c, opd, t)
SETTINGS index_granularity = 8192;

-- «Чистые» данные (целевой слой stage)
DROP TABLE IF EXISTS stg.daily_codes_history ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history ON CLUSTER shardless
AS stg.daily_codes_history_raw
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history', {repl_macro:String})
PARTITION BY toYYYYMMDD(opd)
ORDER BY (c, opd, t)
-- при желании можно дать длинный бизнес-TTL:
TTL toDateTime(opd) + INTERVAL 5 DAY DELETE
SETTINGS index_granularity = 8192;

-- Distributed над «чистыми»
DROP TABLE IF EXISTS stg.daily_codes_history_all ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history_all ON CLUSTER shardless
AS stg.daily_codes_history
ENGINE = Distributed('shardless', 'stg', 'daily_codes_history', cityHash64(c));