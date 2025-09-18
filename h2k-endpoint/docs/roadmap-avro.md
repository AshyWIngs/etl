# Roadmap (Avro / Schema Registry)

> ⚠️ **Статус:** это **дорожная карта**. Сейчас endpoint публикует **только JSON (JSONEachRow)**. Любые новые ключи/параметры из этого файла **не работают**, пока соответствующий код не появится в репозитории. Источник истины — **фактический исходный код**.

---

## 1) Введение

Цель — перевести полезную нагрузку Kafka с JSONEachRow на **Avro** с управляемыми схемами (локально или через **open-source Confluent Schema Registry 5.3.x**) для прироста **скорости, производительности, стабильности и качества**, сохраняя совместимость со стеком:

- Java: **8** (target 1.8)
- HBase: **1.4.13**
- Kafka (клиенты): **2.3.1**
- Phoenix: **4.14/4.15**

---

## 2) Текущее состояние (JSON)

Проект публикует JSONEachRow. Управление типами/нормализацией делает `ValueCodecPhoenix`, схемы колонок — через `SchemaRegistry`/`JsonSchemaRegistry` (+ `schema.json`). PK декодируется в `ValueCodecPhoenix.decodeRowKey(...)`.

Связанные файлы:
- `SchemaRegistry.java` — базовый контракт реестра схем.
- `JsonSchemaRegistry.java` — текущая реализация (единый `schema.json`).

---

## 3) Почему Avro (зачем)

- **Бинарная компактность** → меньше трафика, выше TPS, ниже latency и нагрузка на GC/CPU по сравнению с JSON на объёмах 100M+/день.
- **Строгая типизация** и **эволюция схем** (backward/forward).
- **Confluent-совместимый wire-format**: `magic byte (0) + schemaId + Avro payload`.
- Хорошая интеграция с ClickHouse (`AvroConfluent`) и экосистемой Kafka.

---

## 4) Совместимость стенда (мировые практики)

- **Kafka брокеры/клиенты:** 2.3.1 → совместимы с Schema Registry **5.3.x** (open-source).
- **Java:** 1.8 — ок и для клиента, и для SR 5.3.x.
- **HBase/Phoenix:** на SR напрямую не влияют.
- **ClickHouse 24.8+**: умеет `AvroConfluent` из Kafka.
- **Транспорт**: единообразный (PLAINTEXT/SSL/SASL) для брокеров, SR и потребителей.
- **Время**: синхронизация NTP/PTP на всех узлах (тайм-ауты/метрики).

---

## 5) Жёсткий план внедрения (этапы 0–6)

### ЭТАП 0. Фича-флаг формата, без поломок JSON
**(план, не реализовано)**
- Ключ `h2k.payload.format = json-eachrow|avro` (дефолт: `json-eachrow`).
- Ключ `h2k.avro.mode = generic|confluent` (дефолт: `generic`).
- Проверка значений в `H2kConfig` (IAE при ошибке).

### ЭТАП 1. Единая точка сериализации (Value-only)
**(частично реализуемо без внешних зависимостей)**
- В `PayloadBuilder` ввести интерфейс `PayloadSerializer`.
- `JsonEachRowSerializer` — перенос текущей логики JSON (без изменений).
- `AvroSerializer` — класс-заготовка (пока `UnsupportedOperationException`), чтобы собрать проект и включить фича-флаг без влияния.

### ЭТАП 2. Avro (generic) без внешнего SR
**(реализация)**
- `AvroSerializer` (GenericRecord + `Schema`):
  - логический тип времени — **millis epoch (long)** (Phoenix date/time → long ms).
  - бинарные — `bytes`, строки — `string`, числовые — по фиксированным размерам (mismatch → ISE).
  - reuse `ByteArrayOutputStream`/`DatumWriter`/`Encoder` (меньше GC).
- `AvroSchemaRegistry` (локально): возврат `Schema` по таблице/колонкам; в отсутствие схемы — ISE на русском.
- Никаких сетевых завязок.

### ЭТАП 3. Производительность
- Без лишних копий и аллокаций; предразмеренные коллекции по `h2k.capacity.hints`.
- Никаких `Optional` на горячем пути.
- Проверка профилей Kafka Producer (FAST/BALANCED/RELIABLE) — параметры не меняем, но подтверждаем TPS.

### ЭТАП 4. Confluent-режим (опционально, best practice)
**(план)**
- Wire-format: `0x0 | int32 schemaId | avroBinary`.
- Мини-клиент SR (HTTP) или зависимость SR-клиента (совместимую с Java 8).
- Конфиги (при `confluent`):
  - `h2k.avro.sr.url = http://host1:8081[,http://hostN:8081]`
  - (при необходимости) `h2k.avro.sr.auth.*`
- Subject-нейминг: `${topic}-value` (классика).

### ЭТАП 5. Тесты (JUnit 5, детерминированные)
- Позитив: корректная нормализация `ValueCodecPhoenix`, round-trip Avro (writer/reader).
- Негатив: mismatch фиксированных типов → ISE; нет схемы → ISE; неверные конфиги → IAE.
- Без сети/ФС; Confluent — мок SR.

### ЭТАП 6. Документация и ingest (CH)
- Обновить `docs/clickhouse.md`: раздел «Kafka/Avro» с двумя вариантами:
  - `FORMAT AvroConfluent` + `kafka_schema_registry_url` (при SR).
  - `FORMAT Avro` + `format_schema` (без SR).
- В README — как включить Avro через конфиг (после реализации).

---

## 6) Реализация варианта через Avro (open‑source Schema Registry)
**(план внедрения; ключи пока не работают — см. статус в начале)**

### Развёртывание Schema Registry 5.3.x (open-source)
**Ссылки:**
- Исходники: https://github.com/confluentinc/schema-registry
- Релиз под Kafka 2.3.1: https://github.com/confluentinc/schema-registry/tree/v5.3.8
- README по сборке/запуску: `README.md` в теге `v5.3.8`

**Вариант A — tar из Confluent Platform 5.3.x**
- Развернуть `/opt/schema-registry/{bin,etc,lib}`; логи — `/var/log/schema-registry/`.
- Конфиг `/opt/schema-registry/etc/schema-registry/schema-registry.properties`:
  ```properties
  listeners=http://0.0.0.0:8081
  host.name=${HOSTNAME}
  kafkastore.bootstrap.servers=PLAINTEXT://10.254.3.111:9092,PLAINTEXT://10.254.3.112:9092,PLAINTEXT://10.254.3.113:9092
  kafkastore.topic=_schemas
  kafkastore.topic.replication.factor=3
  kafkastore.timeout.ms=60000
  compatibility.level=BACKWARD
  ```
- Юнит systemd:
  ```ini
  [Unit]
  Description=Schema Registry (open-source)
  After=network-online.target
  [Service]
  Type=simple
  ExecStart=/opt/schema-registry/bin/schema-registry-start /opt/schema-registry/etc/schema-registry/schema-registry.properties
  ExecStop=/opt/schema-registry/bin/schema-registry-stop
  Restart=always
  RestartSec=5
  Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx512m" "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8"
  LimitNOFILE=65536
  [Install]
  WantedBy=multi-user.target
  ```
- Проверка:
  ```bash
  curl -s http://10.254.3.111:8081/subjects   # ожидаем: []
  ```

**Вариант B — сборка из исходников**
```bash
git clone https://github.com/confluentinc/schema-registry.git
cd schema-registry
git checkout v5.3.8
mvn -q -DskipTests package
# далее разложить как в варианте A
```

### Совместимость: короткий чек-лист
- Java 8 на RS/SR/CH; Kafka 2.3.1; `_schemas` с RF≥3; SR доступен для RS/CH.
- `compatibility.level=BACKWARD` или ваша политика.
- В ClickHouse: Kafka-engine с `kafka_format='AvroConfluent'` и `kafka_schema_registry_url='http://host1:8081,...'`.
- Продьюсер (после реализации Avro): включён Avro-serializer + `schema.registry.url=...`.
- Сетевой транспорт единообразен, время синхронизировано.

### Пример Avro‑схемы (TBL_JTI_TRACE_CIS_HISTORY)
`conf/avro/tbl_jti_trace_cis_history.avsc`:
```json
{
  "type": "record",
  "name": "TBL_JTI_TRACE_CIS_HISTORY_Row",
  "namespace": "kz.qazmarka.h2k",
  "fields": [
    {"name": "c", "type": "string"},
    {"name": "t", "type": "int"},
    {"name": "opd", "type": "long"},

    {"name": "id",   "type": ["null","string"], "default": null},
    {"name": "did",  "type": ["null","string"], "default": null},
    {"name": "rid",  "type": ["null","string"], "default": null},
    {"name": "rinn", "type": ["null","string"], "default": null},
    {"name": "rn",   "type": ["null","string"], "default": null},
    {"name": "sid",  "type": ["null","string"], "default": null},
    {"name": "sinn", "type": ["null","string"], "default": null},
    {"name": "sn",   "type": ["null","string"], "default": null},
    {"name": "gt",   "type": ["null","string"], "default": null},
    {"name": "prid", "type": ["null","string"], "default": null},

    {"name": "st",  "type": ["null","int"], "default": null},
    {"name": "ste", "type": ["null","int"], "default": null},
    {"name": "elr", "type": ["null","int"], "default": null},

    {"name": "emd", "type": ["null","long"], "default": null},
    {"name": "apd", "type": ["null","long"], "default": null},
    {"name": "exd", "type": ["null","long"], "default": null},

    {"name": "p",  "type": ["null","string"], "default": null},
    {"name": "pt", "type": ["null","int"], "default": null},
    {"name": "o",  "type": ["null","string"], "default": null},
    {"name": "pn", "type": ["null","string"], "default": null},
    {"name": "b",  "type": ["null","string"], "default": null},
    {"name": "tt", "type": ["null","long"], "default": null},
    {"name": "tm", "type": ["null","long"], "default": null},

    {"name": "ch",  "type": ["null", { "type": "array", "items": "string" }], "default": null},
    {"name": "j",   "type": ["null","string"], "default": null},
    {"name": "pg",  "type": ["null","int"], "default": null},
    {"name": "et",  "type": ["null","int"], "default": null},
    {"name": "pvad","type": ["null","string"], "default": null},
    {"name": "ag",  "type": ["null","string"], "default": null},

    {"name": "event_version", "type": ["null","long"], "default": null},
    {"name": "delete", "type": "boolean", "default": false}
  ]
}
```

**Регистрация в SR** (subject = `<topic>-value`):
```bash
curl -s -X POST http://10.254.3.111:8081/subjects/TBL_JTI_TRACE_CIS_HISTORY-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @conf/avro/tbl_jti_trace_cis_history.avsc
```

### ClickHouse 24.8: чтение AvroConfluent
> DDL приведён как ориентир для ingest; используйте ваш шардирующий/реплицирующий шаблон.

```sql
CREATE TABLE stg.kafka_tbl_jti_trace_cis_history_src
(
  c String,
  t Int32,
  opd Int64,

  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),

  st Nullable(Int32), ste Nullable(Int32), elr Nullable(Int32),

  emd Nullable(Int64), apd Nullable(Int64), exd Nullable(Int64),

  p Nullable(String), pt Nullable(Int32), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm Nullable(Int64),

  ch Array(String), j Nullable(String), pg Nullable(Int32), et Nullable(Int32), pvad Nullable(String), ag Nullable(String),

  event_version Nullable(Int64),
  `delete` UInt8
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
  kafka_topic_list  = 'TBL_JTI_TRACE_CIS_HISTORY',
  kafka_group_name  = 'ch_tbl_jti_trace_cis_history',
  kafka_format      = 'AvroConfluent',
  kafka_schema_registry_url = 'http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081',
  kafka_num_consumers = 6,
  kafka_skip_broken_messages = 1;

CREATE TABLE stg.tbl_jti_trace_cis_history_raw
(
  c String,
  t UInt8,
  opd DateTime64(3, 'UTC'),
  event_version Nullable(DateTime64(3, 'UTC')),
  `delete` UInt8,

  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),

  st Nullable(UInt8), ste Nullable(UInt8), elr Nullable(UInt8),

  emd Nullable(DateTime64(3, 'UTC')), apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
  p Nullable(String), pt Nullable(UInt8), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm Nullable(DateTime64(3, 'UTC')),

  ch Array(String), j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), pvad Nullable(String), ag Nullable(String)
)
ENGINE = MergeTree
ORDER BY (c, t, opd);

CREATE MATERIALIZED VIEW stg.mv_tbl_jti_trace_cis_history_to_raw
TO stg.tbl_jti_trace_cis_history_raw AS
SELECT
  c,
  CAST(t AS UInt8) AS t,
  toDateTime64(opd/1000.0, 3, 'UTC') AS opd,
  ifNull(toDateTime64(event_version/1000.0, 3, 'UTC'), NULL) AS event_version,
  `delete`,
  id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
  CAST(st  AS Nullable(UInt8))  AS st,
  CAST(ste AS Nullable(UInt8))  AS ste,
  CAST(elr AS Nullable(UInt8))  AS elr,
  ifNull(toDateTime64(emd/1000.0, 3, 'UTC'), NULL) AS emd,
  ifNull(toDateTime64(apd/1000.0, 3, 'UTC'), NULL) AS apd,
  ifNull(toDateTime64(exd/1000.0, 3, 'UTC'), NULL) AS exd,
  p, CAST(pt AS Nullable(UInt8)) AS pt, o, pn, b, tt,
  ifNull(toDateTime64(tm/1000.0, 3, 'UTC'), NULL)  AS tm,
  ch, j, CAST(pg AS Nullable(UInt16)) AS pg, CAST(et AS Nullable(UInt8))  AS et, pvad, ag
FROM stg.kafka_tbl_jti_trace_cis_history_src;
```

---

## 7) Изменения в h2k‑endpoint (план; не реализовано)
- Добавить ключи (Этап 0) и стратегии (Этап 1).
- Реализовать `AvroSerializer` (Этап 2) и локальный `AvroSchemaRegistry`.
- (Опция) Confluent-режим (Этап 4) c HTTP‑клиентом к SR.
- Тесты (Этап 5): позитив/негатив, без сети и ФС (моки).

---

## 8) Порядок включения (рекомендуемый)
1. Поднять SR 5.3.x на QA → `/subjects` отвечает.
2. Согласовать и зарегистрировать Avro‑схемы для топиков.
3. Реализовать Этап 2 (generic) и собрать JAR; включить `h2k.payload.format=avro` **только на QA**.
4. В ClickHouse завести Kafka‑источник (AvroConfluent) + RAW + MV; проверить типы/лаг.
5. Нагрузочные тесты; затем включение Confluent‑режима (если требуется SR‑id wire‑format).
6. Постепенно расширять набор таблиц.

---

## 9) Диагностика/эксплуатация
- **Kafka источник (CH):** `SELECT * FROM ..._src LIMIT 5` (+ `SET stream_like_engine_allow_direct_select=1`).
- **RAW:** `SELECT count(), min(opd), max(opd) FROM ..._raw`.
- **Перезапуск с «начала»:** смена `kafka_group_name` или сброс оффсетов у группы.
- **SR:** `GET /subjects`, `GET /subjects/<subj>/versions`, `GET /schemas/ids/<id>`.

---

## Связанные файлы в коде
- `SchemaRegistry.java` — базовый контракт реестра схем.
- `JsonSchemaRegistry.java` — текущая реализация (единый `schema.json`).
- *(план)* `AvroSchemaRegistry.java` — локальные Avro‑схемы (без SR).
- *(план)* `PayloadBuilder` → `PayloadSerializer`/`AvroSerializer`/`JsonEachRowSerializer`.
