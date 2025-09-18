## Прод‑эталон: создание объектов ClickHouse с нуля (Kafka → RAW)

Ниже — **короткая, проверенная инструкция** по созданию трёх объектов для ingest потока `JSONEachRow` из Kafka в ClickHouse **с нуля**:
- Источник Kafka (`ENGINE = Kafka`) — имена полей **как в JSON**, **без** суффиксов `_ms`.
- RAW‑таблица (`ReplicatedMergeTree`) — нормализованные типы `DateTime64(3,'UTC')`, алиасы в `Asia/Almaty`.
- Материализованное представление (MV) — конвертация миллисекунд → `DateTime64(3,'UTC')` и запись в RAW.

> TTL: **только по `event_version`** (бизнес‑время). `event_version` в RAW — **NOT NULL**.

### 0) Предпосылки
- В Kafka летит `JSONEachRow`, времена в **миллисекундах эпохи**: `opd`, `apd`, `emd`, `exd`, `tm`, `event_version` (Int64, мс).  
- Поле `event_version` в потоке — **обязательное**.
- Таймзона хранения — `UTC` (аналитические алиасы — `Asia/Almaty`).

---

### 1) Источник: Kafka (одна таблица на ноду; группа общая на кластер)
```sql
CREATE TABLE stg.kafka_tbl_jti_trace_cis_history_src ON CLUSTER shardless
(
    c String,
    t UInt8,
    opd Int64,

    `delete` UInt8,
    event_version Int64,

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

    emd  Nullable(Int64),
    apd  Nullable(Int64),
    exd  Nullable(Int64),

    p    Nullable(String),
    pt   Nullable(UInt8),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),
    tt   Nullable(Int64),
    tm   Nullable(Int64),

    ch   Array(String),
    j    Nullable(String),
    pg   Nullable(UInt16),
    et   Nullable(UInt8),
    pvad Nullable(String),
    ag   Nullable(String)
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list         = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
  kafka_topic_list          = 'TBL_JTI_TRACE_CIS_HISTORY',
  kafka_group_name          = 'ch_tbl_jti_trace_cis_history',
  kafka_format              = 'JSONEachRow',
  kafka_num_consumers       = 4,
  kafka_max_block_size      = 10000,
  kafka_skip_broken_messages = 1;
```

**Диагностика источника:**
```sql
SET stream_like_engine_allow_direct_select = 1; -- разрешить прямое чтение из Kafka‑таблицы на сессию
SELECT opd, apd, emd, exd, tm, event_version
FROM stg.kafka_tbl_jti_trace_cis_history_src
LIMIT 5;
```

> **Прочитать «с нуля», если раньше уже был консюмер:** используйте **новое** значение `kafka_group_name` (например, добавьте суффикс с датой/временем). Это создаст новую группу и чтение начнётся с earliest (по ретеншну топика). Альтернатива — вручную сбросить оффсеты на брокере для старой группы (через `kafka-consumer-groups.sh`).

---

### 2) RAW: приземление (типизировано) + алиасы и индекс
```sql
CREATE TABLE stg.tbl_jti_trace_cis_history_raw ON CLUSTER shardless
(
    c   String,
    t   UInt8,

    opd DateTime64(3, 'UTC'),
    opd_local DateTime64(3, 'Asia/Almaty') ALIAS toTimeZone(opd, 'Asia/Almaty'),
    opd_local_date Date MATERIALIZED toDate(toTimeZone(opd, 'Asia/Almaty')),

    `delete` UInt8,

    event_version DateTime64(3, 'UTC'),
    event_version_local DateTime64(3, 'Asia/Almaty')
        ALIAS toTimeZone(event_version, 'Asia/Almaty'),

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

    emd  Nullable(DateTime64(3, 'UTC')),
    emd_local  Nullable(DateTime64(3, 'Asia/Almaty')) ALIAS if(isNull(emd), NULL, toTimeZone(emd, 'Asia/Almaty')),

    apd  Nullable(DateTime64(3, 'UTC')),
    apd_local  Nullable(DateTime64(3, 'Asia/Almaty')) ALIAS if(isNull(apd), NULL, toTimeZone(apd, 'Asia/Almaty')),

    exd  Nullable(DateTime64(3, 'UTC')),
    exd_local  Nullable(DateTime64(3, 'Asia/Almaty')) ALIAS if(isNull(exd), NULL, toTimeZone(exd, 'Asia/Almaty')),

    p    Nullable(String),
    pt   Nullable(UInt8),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),
    tt   Nullable(Int64),

    tm   Nullable(DateTime64(3, 'UTC')),
    tm_local  Nullable(DateTime64(3, 'Asia/Almaty')) ALIAS if(isNull(tm), NULL, toTimeZone(tm, 'Asia/Almaty')),

    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),
    j    Nullable(String) CODEC(ZSTD(6)),
    pg   Nullable(UInt16),
    et   Nullable(UInt8),
    pvad Nullable(String) CODEC(ZSTD(6)),
    ag   Nullable(String) CODEC(ZSTD(6)),

    ingested_at DateTime('UTC') DEFAULT now('UTC'),
    ingested_at_local DateTime('Asia/Almaty') ALIAS toTimeZone(ingested_at, 'Asia/Almaty'),

    INDEX idx_opd_local_date (opd_local_date) TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.tbl_jti_trace_cis_history_raw', '{shardless_repl}')
PARTITION BY toYYYYMMDD(opd)
ORDER BY (c, opd, t)
TTL event_version + INTERVAL 5 DAY DELETE
SETTINGS index_granularity = 8192;
```

---

### 3) MV: конвертация JSON‑мс → типы и вставка в RAW
```sql
CREATE MATERIALIZED VIEW stg.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER shardless
TO stg.tbl_jti_trace_cis_history_raw
AS
SELECT
  c,
  t,
  toDateTime64(opd / 1000.0, 3, 'UTC')                                  AS opd,

  `delete`,
  toDateTime64(event_version / 1000.0, 3, 'UTC')                          AS event_version,

  id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
  st, ste, elr,

  ifNull(toDateTime64(emd / 1000.0, 3, 'UTC'), NULL)                      AS emd,
  ifNull(toDateTime64(apd / 1000.0, 3, 'UTC'), NULL)                      AS apd,
  ifNull(toDateTime64(exd / 1000.0, 3, 'UTC'), NULL)                      AS exd,

  p, pt, o, pn, b, tt,
  ifNull(toDateTime64(tm  / 1000.0, 3, 'UTC'), NULL)                      AS tm,

  ch, j, pg, et, pvad, ag
FROM stg.kafka_tbl_jti_trace_cis_history_src;
```

---

## Диагностика / эксплуатация

**Проверка консьюмера:**
```sql
SET stream_like_engine_allow_direct_select = 1;
SELECT * FROM stg.kafka_tbl_jti_trace_cis_history_src LIMIT 5;
```

**Вставки в RAW:**
```sql
SELECT count() FROM stg.tbl_jti_trace_cis_history_raw;
SELECT c, t, opd, apd, emd, tm, exd, event_version
FROM stg.tbl_jti_trace_cis_history_raw
ORDER BY ingested_at DESC
LIMIT 10;
```

**Старт «с нуля», если ранее уже читали топик:**
- Создайте новую `kafka_group_name` (рекомендуется добавлять суффикс даты/времени).  
- Либо сбросьте оффсеты старой группы на брокере (вне ClickHouse) и перезапустите чтение.

**Замечания по производительности:**
- `kafka_num_consumers = 4` — базовый параллелизм чтения; увеличивайте при большом TPS и достаточном числе партиций.
- `kafka_skip_broken_messages = 1` — «плохие» JSON‑строки не блокируют поток (фиксируйте их отдельно через monitoring).
- В RAW `ORDER BY (c,opd,t)` и `index_granularity = 8192` — универсальные настройки для диапазонных запросов по времени и ключу.
