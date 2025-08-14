-- RAW: приземление + TTL по времени загрузки
DROP TABLE IF EXISTS stg.daily_codes_history_raw ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history_raw ON CLUSTER shardless
(
    c   String,
    t   UInt8,
    opd DateTime64(3),

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
    emd  Nullable(DateTime64(3)),
    apd  Nullable(DateTime64(3)),
    exd  Nullable(DateTime64(3)),
    p    Nullable(String),
    pt   Nullable(UInt8),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),
    tt   Nullable(Int64),
    tm   Nullable(DateTime64(3)),
    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),
    j    Nullable(String) CODEC(ZSTD(6)),
    pg   Nullable(UInt16),
    et   Nullable(UInt8),
    pvad Nullable(String) CODEC(ZSTD(6)),
    ag   Nullable(String) CODEC(ZSTD(6)),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history_raw', '{shardless_repl}')
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history_dedup_buf', '{shardless_repl}')
PARTITION BY toYYYYMMDD(opd)
ORDER BY (c, opd, t)
SETTINGS index_granularity = 8192;

-- «Чистые» данные (целевой слой stage)
DROP TABLE IF EXISTS stg.daily_codes_history ON CLUSTER shardless SYNC;
CREATE TABLE stg.daily_codes_history ON CLUSTER shardless
AS stg.daily_codes_history_raw
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history', '{shardless_repl}')
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