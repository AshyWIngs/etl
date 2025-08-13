-- file: ddl/stg_daily_codes_history.sql
-- =========================
-- STG: локальная таблица на шарде
-- =========================
DROP TABLE IF EXISTS stg.daily_codes_history ON CLUSTER shardless SYNC;

CREATE TABLE stg.daily_codes_history ON CLUSTER shardless
(
    -- Обязательные поля (как в источнике)
    c   String,                 -- cis (уникальный код марки)
    t   UInt8,                  -- тип операции
    opd DateTime64(3),          -- дата/время операции (как в источнике, миллисекунды)

    -- Остальные поля один-в-один с источником (NULL, если NULL в источнике)
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

    -- ВАЖНО: массивы в CH не могут быть Nullable → используем Array(String) и DEFAULT []
    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),
    j    Nullable(String) CODEC(ZSTD(6)),

    pg   Nullable(UInt16),
    et   Nullable(UInt8),

    pvad Nullable(String) CODEC(ZSTD(6)),
    ag   Nullable(String) CODEC(ZSTD(6))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history', '{shardless_repl}')
PARTITION BY toYYYYMMDD(opd)             -- дневные партиции (совпадает со слайсами выгрузки)
ORDER BY (c, opd, t)                     -- под типовые запросы (по коду и интервалу дат)
-- TTL: например, удалять старше 180 дней по дате операции
TTL toDateTime(opd) + INTERVAL 180 DAY DELETE   -- <-- явный cast из DateTime64(3) в DateTime
SETTINGS index_granularity = 8192;

-- =========================
-- Распределённая таблица
-- =========================
DROP TABLE IF EXISTS stg.daily_codes_history_all ON CLUSTER shardless SYNC;

CREATE TABLE stg.daily_codes_history_all ON CLUSTER shardless
AS stg.daily_codes_history
ENGINE = Distributed('shardless', 'stg', 'daily_codes_history', cityHash64(c));
/*
  Примечание: cityHash64 здесь только как шард-ключ в распределённой таблице.
  К нашей дедупликации по (c,t,opd) он не относится.
*/