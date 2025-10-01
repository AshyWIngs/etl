# Roadmap (Avro / Schema Registry)

> ⚠️ **Статус:** это дорожная карта **и эксплуатационные заметки**. По умолчанию endpoint публикует **JSON (JSONEachRow)**. Поддержка **Avro (generic)** уже заведена через SPI‑интерфейс сериализации; **Avro Confluent (Schema Registry 5.3.8)** — целевой режим (включается конфигом). Источник истины — код в репозитории и `H2kConfig`.

---

## 1) Цели и совместимость

**Цель:** перевести value‑payload Kafka с JSONEachRow на **Avro** с управляемыми схемами (локально или через open‑source **Confluent Schema Registry 5.3.x**) ради **скорости, производительности, стабильности и качества**, сохранив стек:

- **Java:** 8 (target 1.8)
- **HBase:** 1.4.13
- **Phoenix:** 4.14/4.15 (под HBase‑1.4)
- **Kafka‑клиенты:** 2.3.1
- **Schema Registry (целевой):** 5.3.8 (под Kafka 2.3.1)

ClickHouse: Kafka‑источник понимает `FORMAT AvroConfluent` (рекомендуется) и `FORMAT Avro` (при локальных схемах).

---

## 2) Что уже сделано (в коде)

- **SPI сериализации:** `PayloadSerializer` + фабрика (DIP/ISP).  
- **PayloadBuilder** не знает про формат: делегирует сериализацию выбранному `PayloadSerializer` (SRP/DRY).  
- **JsonEachRowSerializer:** перенос текущей JSON‑логики без функциональных изменений.  
- **AvroSerializer (generic):** сбор `GenericRecord` по Avro‑схеме, бинарная кодировка без лишних аллокаций; сообщения об ошибках на русском.  
- **H2kConfig:** фича‑флаг формата/режима Avro, параметры SR; валидация значений.  
- **pom.xml:** профили под Avro/Confluent, enforcer/locks, страховки от несовместимых транзитивных библиотек.

По умолчанию остаётся **JSON** (обратная совместимость). Avro включается **конфигом**, без перекомпиляции.

---

## 3) План работ (обновлён)

### ЭТАП 0 — Фича‑флаг формата *(выполнено)*
- Ключ `h2k.payload.format = json-eachrow|avro` (дефолт: `json-eachrow`).  
- Ключ `h2k.avro.mode = generic|confluent` (дефолт: `generic`).  
- Проверка и кэш флагов в `H2kConfig` (Perf).

### ЭТАП 1 — Единая точка сериализации *(выполнено)*
- `PayloadSerializer` + фабрика по `payloadFormat/serializerFactoryClass`.  
- Делегирование из `PayloadBuilder` (SRP/ISP/DIP).

### ЭТАП 2 — Avro (generic) без SR *(выполнено)*
- `AvroSerializer`: `GenericData.Record` + `DatumWriter` + повторное использование `ByteArrayOutputStream/Encoder`.  
- Маппинг Phoenix‑типов → Avro (см. §6).

### ЭТАП 3 — Локальный AvroSchemaRegistry *(в процессе)*
- Чтение `conf/avro/*.avsc`, кеш по `TableName/topic`.  
- Диагностика «несовпадений» колонок/nullable.

### ЭТАП 4 — Avro Confluent (Schema Registry) *(целевой)*
- Wire‑format: `0x0 | int32 schemaId | avroBinary`.  
- Клиент SR (HTTP) **или** зависимости `io.confluent` 5.3.8 (Java 8).  
- Политика совместимости: `BACKWARD` (или по требованиям).  
- Subject‑нейминг: `${topic}-value`.

### ЭТАП 5 — Тесты/нагрузка *(итеративно)*
- Unit/IT: позитив (round‑trip), негатив (mismatch типов/нет схемы/плохой конфиг).  
- Нагрузочные на профилях продьюсера (FAST/BALANCED/RELIABLE) — параметры не меняем.

### ЭТАП 6 — Документация/интеграции *(итеративно)*
- ClickHouse ingest (Kafka‑engine) для AvroConfluent/Avro; RAW+MV.

---

## 4) Конфигурация (ключи) и сценарии

> Точные имена и дефолты — в `H2kConfig`. Ниже — логика и ожидаемые значения.

### 4.1 Базовые ключи
- `h2k.payload.format = json-eachrow | avro`  
- `h2k.serializerFactoryClass = <FQN фабрики>`  
- `h2k.avro.mode = generic | confluent`

### 4.2 Confluent‑режим (`avro` + `confluent`)
- `h2k.avro.sr.urls = http://host1:8081[,http://hostN:8081]`  
- (опц.) `h2k.avro.sr.auth.*` (Basic/OAuth/MTLS)  
- Subject‑нейминг: `${topic}-value` (дефолт)

### 4.3 Как включить Avro (generic)
1) Положить `.avsc` в `conf/avro/` (см. §7).  
2) Установить:
   ```
   h2k.payload.format=avro
   h2k.avro.mode=generic
   ```
3) Перезапустить peer. Проверить метрики/логи.

### 4.4 Как включить Avro Confluent
1) Поднять SR 5.3.8; зарегистрировать схемы (см. §8).  
2) Установить:
   ```
   h2k.payload.format=avro
   h2k.avro.mode=confluent
   h2k.avro.sr.urls=http://host1:8081,http://host2:8081
   ```
3) Перезапустить peer. Проверить, что value начинается с `0x0|schemaId|…`.

---

## 5) Производительность/стабильность (инварианты)

- **Нулевая магия:** `PayloadBuilder` подаёт типизированные поля; сериализатор только кодирует.  
- **Повторное использование буферов** в Avro‑кодеке; без лишних копий.  
- **Прогретые флаги** в `H2kConfig` (rowkeyBase64 и др.).  
- **Профили Kafka продьюсера** (FAST/BALANCED/RELIABLE) — без изменений.  
- **Ошибки/логирование** — на русском, без «тихих» падений.

---

## 6) Маппинг Phoenix → Avro (правила)

- `TIMESTAMP/DATE/TIME` → `long` (epoch‑millis).  
- `UNSIGNED_TINYINT/SMALLINT/INT` → `int`/`long` по диапазону (строго).  
- `VARCHAR/CHAR` → `string` (UTF‑8).  
- `BINARY/VARBINARY` → `bytes`.  
- Массивы → `{"type":"array","items":<T>}`.  
- Nullable → `["null",<T>]` + `"default": null`.  
- Любой mismatch (фикс‑размеры/nullability) → **ISE**.

Пример живой схемы колонок/PK для `TBL_JTI_TRACE_CIS_HISTORY` смотрите в `schema.json`: первичный ключ `["c","t","opd"]`, а типы колонок соответствуют Phoenix‑описанию.

---

## 7) Локальный AvroSchemaRegistry и `.avsc`

- Путь: `conf/avro/<table>.avsc`.  
- Требования к полям:
  - имена/порядок согласованы с `SchemaRegistry`/`schema.json`;  
  - времена — **long epoch‑ms**;  
  - бинарные — `bytes`; флаг _delete — `boolean/UInt8` на стороне CH.  
- Кеш по `TableName`/`topic`; on‑miss → детальная ошибка на русском.

---

## 8) Schema Registry 5.3.8 (open‑source)

Развёртывание: tar из Confluent Platform или сборка из исходников `v5.3.8` (Java 8). Базовые настройки: `listeners`, `kafkastore.bootstrap.servers`, `_schemas` (RF≥3), `compatibility.level=BACKWARD`.  

Регистрация схем: subject = `${topic}-value`, версионирование по SR API.  

Эксплуатация: `GET /subjects`, `…/versions`, `…/schemas/ids/<id>`; мониторинг доступности, latency, ошибок совместимости.

---

## 9) ClickHouse (ingest через Kafka‑engine)

- Рекомендуемый формат — `AvroConfluent` + `kafka_schema_registry_url`.  
- Pipeline: Kafka‑источник (сырой Avro) → RAW таблица (нормализация времени/беззнаковых) → MV → целевая.  
- DDL‑пример и маппинг типов приведены ниже; адаптируйте `ORDER BY`/шардинг.

```sql
-- Kafka‑источник
CREATE TABLE stg.kafka_tbl_jti_trace_cis_history_src
(
  c String, t Int32, opd Int64,
  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),
  st Nullable(Int32), ste Nullable(Int32), elr Nullable(Int32),
  emd Nullable(Int64), apd Nullable(Int64), exd Nullable(Int64),
  p Nullable(String), pt Nullable(Int32), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm Nullable(Int64),
  ch Array(String), j Nullable(String), pg Nullable(Int32), et Nullable(Int32), pvad Nullable(String), ag Nullable(String),
  _event_ts Nullable(Int64), _delete UInt8
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '<brokers>',
  kafka_topic_list  = 'TBL_JTI_TRACE_CIS_HISTORY',
  kafka_group_name  = 'ch_tbl_jti_trace_cis_history',
  kafka_format      = 'AvroConfluent',
  kafka_schema_registry_url = 'http://host1:8081,http://host2:8081',
  kafka_num_consumers = 6,
  kafka_skip_broken_messages = 1;

-- RAW
CREATE TABLE stg.tbl_jti_trace_cis_history_raw
(
  c String, t UInt8, opd DateTime64(3, 'UTC'),
  _event_ts Nullable(DateTime64(3, 'UTC')), _delete UInt8,
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
  ifNull(toDateTime64(_event_ts/1000.0, 3, 'UTC'), NULL) AS _event_ts,
  _delete,
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

## 10) Включение по средам и откат

1) **QA:** Avro (generic) на 1–2 топика, канареечная группа CH; сравнение лагов/TPS/ошибок.  
2) **QA:** Avro Confluent, та же выборка; проверка схем/совместимости.  
3) **UAT/PROD:** по батчам таблиц; контроль объёма/лага.  
**Откат:** вернуть `h2k.payload.format=json-eachrow` и перезапустить peer; Kafka‑данные сохранены.

---

## 11) Диагностика/наблюдаемость

- **Логи endpoint:** выбранный сериализатор/формат, имя схемы/subject, schemaId (confluent), размеры записей.  
- **Метрики:** длина батча, размер payload, время сериализации/отправки, ретраи.  
- **CH:** `SELECT * FROM ..._src LIMIT 5` (+ `SET stream_like_engine_allow_direct_select=1`).

---

## 12) Частые ошибки и решения

- **Нет схемы (generic):** проверьте `conf/avro/*.avsc` и имя таблицы/топика.  
- **Несовместимость типов:** сравните `.avsc` и описание колонок в `schema.json` (особенно epoch‑ms для времени/беззнаковые). fileciteturn3file2  
- **SR id недоступен:** проверьте `h2k.avro.sr.urls`, сеть/аутентикацию и наличие subject’а.  
- **Конфликт зависимостей:** сборка профильным `pom.xml` (enforcer/locks).

---

## 13) Связанные файлы и роли

- `PayloadBuilder` — сбор полей и делегирование сериализатору.  
- `PayloadSerializer`, `JsonEachRowSerializer`, `AvroSerializer`.  
- `SchemaRegistry.java`, `JsonSchemaRegistry.java` (JSON ветка), *(план)* `AvroSchemaRegistry.java` (локальные Avro‑схемы).  
- `H2kConfig` — чтение ключей/инварианты.  
- `KafkaReplicationEndpoint` — оркестрация (формат прозрачен).  
- `BatchSender` — отправка батчами.

---

## 14) История и источники

Этот документ — обновлённая версия первоначальной дорожной карты, где статус фич отражал только JSON‑режим. Для истории изменений и ранних предпосылок см. предыдущую редакцию roadmap.