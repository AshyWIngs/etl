# HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (JSONEachRow)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Endpoint‑класс:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x, публикующий изменения строк в Kafka как **одну JSON‑строку на событие** (формат JSONEachRow). Код и конфиги ориентированы на минимальные аллокации и высокую пропускную способность, с приоритетом стабильности.

---

## Содержание

- [Чек-лист совместимости для Avro](#avro-compat-checklist)

## Быстрый старт

1) **Соберите и разложите JAR** на все RegionServer:
   ```bash
   mvn -q -DskipTests clean package
   cp target/h2k-endpoint-*.jar /opt/hbase-default-current/lib/
   ```
2) **Подготовьте схему Phoenix** (если используете `json-phoenix`), файл:
   `/opt/hbase-default-current/conf/schema.json` (см. раздел «Схема Phoenix»).
3) **Включите репликацию CF** в нужных таблицах (см. раздел «Включение репликации нужных CF» ниже). Также убедитесь, что в глобальном `hbase-site.xml` включена репликация кластера:
   ```
   <property><name>hbase.replication</name><value>true</value></property>
   ```
4) **Создайте peer** готовым скриптом (HBase 1.4.13), выберите профиль:
   - `conf/add_peer_shell_fast.txt` — FAST (макс. скорость)
   - `conf/add_peer_shell_balanced.txt` — BALANCED (компромисс)
   - `conf/add_peer_shell_reliable.txt` — RELIABLE (надёжность)
   
   Запуск: `bin/hbase shell conf/add_peer_shell_fast.txt`  
   Подробности: см. раздел «Профили peer (готовые скрипты)».
5) **Проверьте доставку**: сообщения появляются в Kafka‑топике `${table}`.

## Поддерживаемые версии

- **Java:** 8 (target 1.8)
- **HBase:** 1.4.13 (совместимо с 1.4.x)
- **Kafka (клиенты):** 2.3.1
- **Phoenix:** 4.14/4.15 для HBase‑1.4 (опционально, для режима `json-phoenix`)

---

## Сборка

```bash
mvn -q -DskipTests clean package
# Артефакт: target/h2k-endpoint-${project.version}.jar
```

**Тесты (опционально):**

```bash
mvn -q test                      # все тесты
mvn -q test -Dtest=Value*Test    # выборочно
```

---

## Деплой

1. Скопируйте JAR на **все RegionServer** в каталог **`/opt/hbase-default-current/lib/`**.  
   Если используете другой путь — добавьте его в `HBASE_CLASSPATH`.
2. Убедитесь, что на RS установлены зависимости **с `scope=provided`**:
   - `kafka-clients-2.3.1.jar`  (Для всех peer)
   - `lz4-java-1.6.0+.jar`      (Только для peer FAST (макс. скорость) и BALANCED (компромисс))
   - `snappy-java-1.1.x+.jar`   (Только для peer RELIABLE (надёжность), если используете `compression.type=snappy`)
   Проверка:
   ```bash
   hbase classpath | tr ':' '\n' | egrep -i 'kafka-clients|lz4|snappy'
   ```
   При отсутствии — скопируйте из каталога Kafka в `/opt/hbase-default-current/lib/` и перезапустите RS.
3. Перезапустите RegionServer.

**Размещение в продакшене (рекомендации):**

- JAR: `/opt/hbase-default-current/lib/`
- HBase‑конфиги: `/opt/hbase-default-current/conf/`
- Схема Phoenix (если включена): `/opt/hbase-default-current/conf/schema.json` и ключ `h2k.schema.path=/opt/hbase-default-current/conf/schema.json`

---

## Конфигурация: где задавать ключи

Используются **оба** источника:

1) **Системный** `hbase-site.xml` (на RS) — базовые значения по умолчанию.  
2) **Конфиг peer** (через HBase shell API) — **имеет приоритет**, удобно для разных пир‑профилей.

Рекомендация: **не заменять** штатный `hbase-site.xml`, а добавлять свои ключи с префиксом `h2k.*`. Для таблиц из `DEFAULT`‑неймспейса имена допускаются без префикса `DEFAULT.` (например: `TBL_JTI_TRACE_CIS_HISTORY`).

*Примечание про `DEFAULT`‑неймспейс.*  
В HBase shell таблицы из `DEFAULT` указываются **без префикса** (`TBL...`). В Phoenix SQL также используйте просто имя без `DEFAULT.` — запись `DEFAULT.TBL...` не поддерживается и приведёт к ошибке парсера.

---

## Ключи `h2k.*`


### Кратко: минимально достаточный набор для запуска

Если вы реплицируете `TBL_JTI_TRACE_CIS_HISTORY` (CF `d`) в режиме Phoenix‑декодера:
```properties
h2k.kafka.bootstrap.servers=10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092
h2k.cf.list=d
h2k.decode.mode=json-phoenix
h2k.schema.path=/opt/hbase-default-current/conf/schema.json
h2k.salt.map=TBL_JTI_TRACE_CIS_HISTORY=1
h2k.capacity.hints=TBL_JTI_TRACE_CIS_HISTORY=32
# опционально:
h2k.ensure.topics=true
h2k.topic.pattern=${table}
```

#### Мини‑таблица: «минимум для запуска» → где применяется

| Ключ | Где применяется | Назначение |
|---|---|---|
| `h2k.kafka.bootstrap.servers` | **KafkaReplicationEndpoint → KafkaProducer** | Список брокеров Kafka |
| `h2k.cf.list` | **PayloadBuilder** | Какие CF экспортируем (через запятую) |
| `h2k.decode.mode` | **KafkaReplicationEndpoint** | Режим декодирования: `simple` или `json-phoenix` |
| `h2k.schema.path` | **JsonSchemaRegistry** | Путь к `schema.json` (нужен только для `json-phoenix`) |
| `h2k.salt.map` | **PayloadBuilder / Decoder** | Длина префикса соли для «salted» таблиц (у Phoenix всегда `1`) |
| `h2k.capacity.hints` | **PayloadBuilder** | Подсказка ёмкости корневого JSON‑объекта (избегаем расширений) |


## Таблица ключей (сводно)

| Ключ | Дефолт | Единицы | Где применяется | Назначение / примечание |
|---|---|---|---|---|
| `h2k.kafka.bootstrap.servers` | — | `host:port` через запятую | KafkaReplicationEndpoint → KafkaProducer | Список брокеров Kafka |
| `h2k.topic.pattern` | `${table}` | шаблон | KafkaReplicationEndpoint | Шаблон имени топика. Плейсхолдеры: \`\${namespace}\`, \`\${qualifier}\`, \`\${table}\` (если \`namespace=default\` → \`\${qualifier}\`, иначе \`\${namespace}_\${qualifier}\`). |
| `h2k.cf.list` | — | CSV | PayloadBuilder | Список CF для экспорта (`d,b,0`). Не существующие CF игнорируются без ошибок |
| `h2k.decode.mode` | `simple` | enum | KafkaReplicationEndpoint | Режим декодирования: `simple` или `json-phoenix` |
| `h2k.schema.path` | — | путь | JsonSchemaRegistry | Путь к единственному файлу `schema.json`. Используется только в режиме `json-phoenix`. |
| `h2k.json.serialize.nulls` | `false` | boolean | Gson в Endpoint | Добавлять ли `null` в JSON |
| `h2k.payload.include.meta` | `false` | boolean | PayloadBuilder | Добавлять служебные поля (+8 ключей) |
| `h2k.payload.include.meta.wal` | `false` | boolean | PayloadBuilder | Добавлять WAL‑метаданные (+2 ключа) |
| `h2k.payload.include.rowkey` | `false` | boolean | PayloadBuilder | Включать `_rowkey` (+1 ключ) |
| `h2k.rowkey.encoding` | `BASE64` | enum | PayloadBuilder | Формат rowkey: `BASE64` или `HEX` (используется только если `include.rowkey=true`) |
| `h2k.filter.by.wal.ts` | `false` | boolean | KafkaReplicationEndpoint | Включить фильтрацию по минимальному WAL‑времени |
| `h2k.wal.min.ts` (мс) | `-1` | миллисекунды epoch | KafkaReplicationEndpoint | Минимальный `timestamp`; применяется при `filter.by.wal.ts=true` |
| `h2k.salt.map` (байты префикса) | — | `TABLE=bytes` (CSV) | PayloadBuilder / Decoder | Длина префикса соли per‑table. Для Phoenix‑salted (`SALT_BUCKETS>0`) всегда `1` |
| `h2k.capacity.hints` (ключи) | — | `TABLE=keys` (CSV) | PayloadBuilder | Подсказка ёмкости корневого JSON (ожидаемое число ключей) |
| `h2k.producer.await.every` (шт.) | `500` | отправок | BatchSender | Порог дозированного ожидания подтверждений |
| `h2k.producer.await.timeout.ms` (мс) | `180000` | миллисекунды | BatchSender | Таймаут ожидания группы futures |
| `h2k.producer.batch.counters.enabled` | `false` | boolean | BatchSender | Внутренние счётчики (DEBUG) |
| `h2k.producer.batch.debug.on.failure` | `false` | boolean | BatchSender | DEBUG‑диагностика ошибок авто‑сброса |
| `h2k.ensure.topics` | `true` | boolean | TopicEnsurer | Автопроверка/создание тем |
| `h2k.topic.partitions` (шт.) | — | число | TopicEnsurer | Число партиций при создании темы |
| `h2k.topic.replication` (фактор) | — | число | TopicEnsurer | Фактор репликации при создании темы |
| `h2k.topic.config.*` | — | ключи Kafka | TopicEnsurer | Свойства создаваемой темы (pass‑through) |
| `h2k.admin.timeout.ms` (мс) | `30000` | миллисекунды | TopicEnsurer | Таймаут операций AdminClient |
| `h2k.log.dir` | `${hbase.log.dir}` или `./logs` | путь | Логирование | Каталог логов endpoint |
| `h2k.log.maxFileSize` (байты/строка) | `64MB` | строка | Логирование | Максимальный размер файла лога (RollingFileAppender) |
| `h2k.log.maxBackupIndex` | `10` | число | Логирование | Количество архивных лог‑файлов |
| `h2k.producer.acks` | 1 | enum | KafkaProducer | Уровень подтверждений: 0/1/all. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.enable.idempotence` | false | boolean | KafkaProducer | Идемпотентность продьюсера. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.max.in.flight` (шт.) | 5 | число | KafkaProducer | Максимум запросов «в полёте» на одно соединение. Поддерживается только ключ `h2k.producer.max.in.flight`. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.linger.ms` (мс) | 0 | миллисекунды | KafkaProducer | Задержка на набор батча перед отправкой. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.batch.size` (байты) | 16384 | байты | KafkaProducer | Целевой размер батча. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.compression.type` | none | enum | KafkaProducer | Тип компрессии: `lz4`/`snappy`/`none`. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.retries` (шт.) | 2147483647 | число | KafkaProducer | Количество ретраев при временных ошибках (ограничено `delivery.timeout.ms`). [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.request.timeout.ms` (мс) | 30000 | миллисекунды | KafkaProducer | Таймаут одного RPC к брокеру. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.delivery.timeout.ms` (мс) | 120000 | миллисекунды | KafkaProducer | Общий дедлайн доставки записи. [см. матрицу](#матрица-профилей-ключевые-отличия) |
| `h2k.producer.buffer.memory` (байты) | 33554432 | байты | KafkaProducer | Объём внутреннего буфера продьюсера |
| `h2k.producer.max.request.size` (байты) | 1048576 | байты | KafkaProducer | Максимальный размер одного запроса к брокеру |
| `h2k.producer.client.id` | не задан | строка | KafkaProducer | Идентификатор клиента; по умолчанию не задаём — endpoint сам формирует уникальный (hostname → UUID фолбэк) |
| `h2k.producer.*` | как в Kafka 2.3.1 | — | KafkaProducer | Любые нативные свойства Kafka Producer (pass‑through) |

**Примечания:**
- Все значения «Дефолт» в таблице указаны для Kafka 2.3.1 (клиент).
- Размеры (`*.size`, `buffer.memory`, `max.request.size`) указаны в байтах; параметры `*.ms` — в миллисекундах.

## Именование топиков: как формируется имя

Имя топика получается как интерполяция шаблона `h2k.topic.pattern` по данным HBase:

- `namespace` = `TableName.getNamespaceAsString()`; для дефолтного неймспейса это строка `"default"`.
- `qualifier` = `TableName.getQualifierAsString()`; это собственно имя таблицы **без** неймспейса.
- `${table}` — удобный шорткат:  
  если `namespace == "default"` → берётся только `${qualifier}`;  
  иначе → `${namespace}_${qualifier}`.  
  Если нужен другой формат — сформируйте его прямо в шаблоне (например, `${namespace}.${qualifier}`).

**Примеры (при `h2k.topic.pattern=${table}`):**
- `DEFAULT`:`TBL_JTI_TRACE_CIS_HISTORY` → топик **`TBL_JTI_TRACE_CIS_HISTORY`**
- `WORK`:`CIS_HISTORY` → топик **`WORK_CIS_HISTORY`**

**Если создаёте топики вручную**, убедитесь, что имена совпадают с тем, что вычислится из шаблона.  
Или задайте явный шаблон, например:
- `${qualifier}` — всегда только имя таблицы;
- `${namespace}.${qualifier}` — через точку;
- `hbase_${namespace}__${qualifier}` — с префиксом.

**Автосоздание тем (`h2k.ensure.topics=true`):**
- Создание выполняет `TopicEnsurer` при первом обращении к таблице.
- Используются параметры `h2k.topic.partitions`, `h2k.topic.replication` и любые `h2k.topic.config.*`.
- Валидные имена Kafka: символы `[a-zA-Z0-9._-]`, длина 1..249; запрещены `'.'` и `'..'`.

### Устаревшие ключи

- `h2k.rowkey.base64` — не используется. Вместо него применяйте `h2k.rowkey.encoding=BASE64|HEX`.

---

## Подсказки ёмкости и метаданные (как выбрать значения)

**Зачем нужны `h2k.capacity.hints`:** PayloadBuilder создаёт корневой `LinkedHashMap` с заранее рассчитанной ёмкостью, чтобы **избежать внутренних расширений** и лишних копирований. Это заметно снижает нагрузку GC на больших сообщениях.

**Как считать hint для таблицы:**  
Возьмите «типичный максимум не‑`null` полей» в ваших данных **по выбранным CF**, и **если включены мета‑поля**, добавьте:

- если `h2k.payload.include.meta=true` → `+8` ключей: `_table,_namespace,_qualifier,_cf,_cells_total,_cells_cf,event_version,delete`;
- если `h2k.payload.include.meta.wal=true` → `+2` ключа: `_wal_seq,_wal_write_time`;
- если `h2k.payload.include.rowkey=true` → `+1` ключ: `_rowkey` (формат по `h2k.rowkey.encoding`: `BASE64` или `HEX`).

**Пример:**  
Таблица `TBL_JTI_TRACE_CIS_HISTORY` имеет 32 логических поля.  
При текущих настройках (`h2k.payload.include.meta=false`, `h2k.payload.include.meta.wal=false`, `h2k.payload.include.rowkey=false`) разумный hint — **`32`**.  
Если позже включите базовые мета‑поля и rowkey, станет `32 + 8 + 1 = 41`. Для запаса можно округлять вверх до ближайшей «красивой» величины (например, 44).

**Где задавать:**  
В peer‑конфиге (имеет приоритет) или в `hbase-site.xml`:
```
# одно значение или список через запятую
h2k.capacity.hints = TBL_JTI_TRACE_CIS_HISTORY=32,AGG.INC_DOCS_ACT=18
```

**О соли (`h2k.salt.map`):**  
Если таблица **Phoenix‑salted** (`SALT_BUCKETS > 0`), префикс соли занимает **ровно 1 байт**.  
Укажите `table=1` в `h2k.salt.map`, чтобы PayloadBuilder/Decoder корректно работали с rowkey.  
Для таблиц без соли задавать ничего не нужно.

---


### Включение репликации нужных CF (в HBase shell)

```HBase shell
# Пример: TBL_JTI_TRACE_CIS_HISTORY, включаем CF 'd'
disable 'TBL_JTI_TRACE_CIS_HISTORY'
alter  'TBL_JTI_TRACE_CIS_HISTORY', { NAME => 'd', REPLICATION_SCOPE => 1 }
enable 'TBL_JTI_TRACE_CIS_HISTORY'
```

```HBase shell
# Пример: таблица RECEIPT, включаем 'b' и 'd'
disable 'RECEIPT'
alter  'RECEIPT', { NAME => 'b', REPLICATION_SCOPE => 1 }
alter  'RECEIPT', { NAME => 'd', REPLICATION_SCOPE => 1 }
enable 'RECEIPT'

# Пример: DOCUMENTS с CF '0' и 'DOCUMENTS'
disable 'DOCUMENTS'
alter  'DOCUMENTS', { NAME => '0',         REPLICATION_SCOPE => 1 }
alter  'DOCUMENTS', { NAME => 'DOCUMENTS', REPLICATION_SCOPE => 1 }
enable 'DOCUMENTS'
```

## Профили peer (готовые скрипты)

Скрипты для создания peer находятся в каталоге `conf/`:
- FAST — `conf/add_peer_shell_fast.txt` (максимальная скорость)
- BALANCED — `conf/add_peer_shell_balanced.txt` (компромисс)
- RELIABLE — `conf/add_peer_shell_reliable.txt` (строгие гарантии)

Как запускать:
```bash
# выполнить скрипт целиком
bin/hbase shell conf/add_peer_shell_fast.txt

# или открыть, скопировать и вставить содержимое в интерактивный shell
bin/hbase shell
```

Проверка:
```HBase shell
list_peers
show_peer_tableCFs 'kafka_peer_fast'   # вернёт nil, если ограничений нет
```

---

#### Ограничить набор таблиц/CF на стороне HBase (опционально)
По умолчанию `TABLE_CFS = nil` → HBase отдаёт в Endpoint все таблицы/CF, у которых `REPLICATION_SCOPE => 1`. 
Чтобы сузить поток **ещё на стороне HBase**, используйте:
```
# только DOCUMENTS:0 и RECEIPT:b,d
set_peer_tableCFs 'kafka_peer_fast', 'DOCUMENTS:0;RECEIPT:b,d'
show_peer_tableCFs 'kafka_peer_fast'   # проверка
```
Это уменьшает объём обрабатываемых WALEdit до того, как они попадут в Endpoint.

---

## Матрица профилей (ключевые отличия)

Ниже сводная таблица ключей, которые различаются между профилями. Единицы измерения: `*.ms` — миллисекунды; размеры (`batch.size`, `buffer.memory`, `max.request.size`) — байты.

| Ключ | FAST (мс/байты) | BALANCED (мс/байты) | RELIABLE (мс/байты) |
|---|---:|---:|---:|
| h2k.producer.acks | 1 | all | all |
| h2k.producer.enable.idempotence | false | true | true |
| h2k.producer.max.in.flight | 5 | 3 | 1 |
| h2k.producer.linger.ms | 100 | 100 | 50 |
| h2k.producer.batch.size | 524288 | 131072 | 65536 |
| h2k.producer.compression.type | lz4 | lz4 | snappy |
| h2k.producer.retries | 10 | 2147483647 | 2147483647 |
| h2k.producer.request.timeout.ms | 30000 | 60000 | 120000 |
| h2k.producer.delivery.timeout.ms | 90000 | 300000 | 300000 |
| h2k.producer.buffer.memory | 268435456 | 268435456 | 268435456 |
| h2k.producer.max.request.size | 2097152 | 2097152 | 2097152 |
| h2k.producer.await.every | 500 | 500 | 500 |
| h2k.producer.await.timeout.ms | 180000 | 300000 | 300000 |

Пояснения:
- FAST: приоритет throughput (acks=1, без идемпотентности), крупные батчи и агрессивный параллелизм.
- BALANCED: строгие подтверждения и идемпотентность при умеренном параллелизме; типовой продакшен.
- RELIABLE: максимум гарантий и порядка (in-flight=1, меньшие батчи, компрессор snappy).
Дополнение: значение `retries=2147483647` в профилях BALANCED/RELIABLE трактуем как «практически безлимитные повторы до дедлайна `delivery.timeout.ms`».

---

## Формат сообщения (JSONEachRow)

_Пример ниже — реальная строка из `TBL_JTI_TRACE_CIS_HISTORY` (PK: `c` VARCHAR, `t` UNSIGNED_TINYINT, `opd` TIMESTAMP)._
Пример получен при включённом `include.meta=true`; при `include.meta=false` поля `event_version` и `delete` отсутствуют.
На каждую строку (rowkey) — одна JSON‑строка:

```json
{
  "c": "00000046199775'I(Nkeb",
  "t": 1,
  "opd_ms": 1749817651300,
  "event_version": 1749817651301,
  "delete": false,

  "did": "054ac16f-e8ef-432f-9b66-852bf9c322dd",
  "sid": "981204350853",
  "sinn": "981204350853",
  "gt": "00000046199775",
  "prid": "981204350853",
  "st": 1,
  "emd_ms": 1749817524733,
  "apd_ms": 1749817651300,
  "pt": 0,
  "o": "981204350853",
  "tm_ms": 1749817651301,
  "j": "{\"srid\":\"054ac16f-e8ef-432f-9b66-852bf9c322dd\",\"hash\":\"UTILISATION_REPORT$e84a7ef9f9ce4a80a969dc02e8d979f9\",\"plid\":\"4\",\"lastChangeBy\":\"08e8a8fb-ecfa-4a01-9882-7a1bf1730a83\"}",
  "pg": 3,
  "et": 1
}
```

- `c`, `t`, `opd_ms` — части PK из Phoenix rowkey (`VARCHAR`, `UNSIGNED_TINYINT`, `TIMESTAMP` → миллисекунды).
- `event_version` — максимум меток времени среди ячеек выбранного CF; **в примере** совпадает с `tm_ms` строки.
- `delete=true` — если в партии был delete‑маркер по CF; иначе `false`.
- Прочие поля — это значения колонок из CF, приведённые по Phoenix‑типам; все `TIMESTAMP` сериализуются как epoch‑millis (`*_ms`). Поля с `NULL` по умолчанию опускаются (см. `h2k.json.serialize.nulls`).

---

## Схема Phoenix (`conf/schema.json`)

В режиме `h2k.decode.mode=json-phoenix` endpoint использует компактное описание таблиц (карта *таблица → {columns}*), чтобы строго и быстро привести байтовые значения к типам Phoenix.

- Ключ таблицы — `NAMESPACE.TABLE`. Для таблиц из `DEFAULT` неймспейса указывайте просто `TABLE` (без `DEFAULT.`).
- Ключи в `columns` — **имена колонок/квалифаеров** в том виде, как они лежат в HBase (регистр важен).
- Значение — тип Phoenix в `UPPER` (`VARCHAR`, `UNSIGNED_TINYINT`, `TIMESTAMP`, `BIGINT`, `...`, а также `... ARRAY`).

**Как указывать таблицы с неймспейсом (Phoenix schema):**
- Ключ таблицы в `schema.json` — это **HBase namespace + '.' + имя таблицы**.  
  Пример: `WORK.CIS_HISTORY`.
- Не используйте кавычки из SQL DDL: запись `"WORK".CIS_HISTORY` в `schema.json` **неверна**.  
  Кавычки — это синтаксис Phoenix для SQL, а не часть имени.
- Для таблиц из `DEFAULT` неймспейса указывайте просто `TABLE` (без `DEFAULT.`).

Пример с двумя таблицами (DEFAULT и WORK):

```json
{
  "TBL_JTI_TRACE_CIS_HISTORY": {
    "columns": {
      "c": "VARCHAR",
      "t": "UNSIGNED_TINYINT",
      "opd": "TIMESTAMP"
    }
  },
  "WORK.CIS_HISTORY": {
    "columns": {
      "tm": "TIMESTAMP",
      "c":  "VARCHAR",
      "t":  "UNSIGNED_TINYINT",
      "opd":"TIMESTAMP"
    }
  }
}
```

> Важно: `schema.json` нужен только для **декодирования типов**.  
> Имя топика берётся из фактической таблицы в событии WAL и шаблона `h2k.topic.pattern`; содержимое `schema.json` **не влияет** на выбор имени топика.

Пример (реальная таблица `TBL_JTI_TRACE_CIS_HISTORY`):

```json
{
  "TBL_JTI_TRACE_CIS_HISTORY": {
    "columns": {
      "c": "VARCHAR",
      "t": "UNSIGNED_TINYINT",
      "opd": "TIMESTAMP",
      "id": "VARCHAR",
      "did": "VARCHAR",
      "rid": "VARCHAR",
      "rinn": "VARCHAR",
      "rn": "VARCHAR",
      "sid": "VARCHAR",
      "sinn": "VARCHAR",
      "sn": "VARCHAR",
      "gt": "VARCHAR",
      "prid": "VARCHAR",
      "st": "UNSIGNED_TINYINT",
      "ste": "UNSIGNED_TINYINT",
      "elr": "UNSIGNED_TINYINT",
      "emd": "TIMESTAMP",
      "apd": "TIMESTAMP",
      "p": "VARCHAR",
      "pt": "UNSIGNED_TINYINT",
      "o": "VARCHAR",
      "pn": "VARCHAR",
      "b": "VARCHAR",
      "tt": "BIGINT",
      "tm": "TIMESTAMP",
      "ch": "VARCHAR ARRAY",
      "j": "VARCHAR",
      "pg": "UNSIGNED_SMALLINT",
      "et": "UNSIGNED_TINYINT",
      "exd": "TIMESTAMP",
      "pvad": "VARCHAR",
      "ag": "VARCHAR"
    }
  }
}
```

## Логирование

Мы используем Log4j с консольным выводом и ротацией файлов (RollingFileAppender).

**По умолчанию**

- Кодировка: UTF‑8 (русские сообщения без проблем).
- Паттерн: `%d{ISO8601} %-5p [%t] %c - %m%n` (без дорогих `%M/%L`).
- Файл лога: `${h2k.log.dir}/h2k-endpoint.log`. Если `-Dh2k.log.dir` не задан, берётся `${hbase.log.dir}`; при отсутствии и этого — `./logs`.
- Ротация:
  - размер файла: `${h2k.log.maxFileSize}` (по умолчанию `64MB`);
  - число бэкапов: `${h2k.log.maxBackupIndex}` (по умолчанию `10`).

**Как задать каталог и ротацию (через HBASE_OPTS):**

```bash
export HBASE_OPTS="$HBASE_OPTS -Dh2k.log.dir=/opt/hbase-default-current/logs -Dh2k.log.maxFileSize=128MB -Dh2k.log.maxBackupIndex=20"
```

**systemd override (для RegionServer):**

```ini
[Service]
Environment="HBASE_OPTS=${HBASE_OPTS} -Dh2k.log.dir=/opt/hbase-default-current/logs -Dh2k.log.maxFileSize=128MB -Dh2k.log.maxBackupIndex=20"
```

**Уровни логов:**

- Внешние библиотеки — `WARN` по умолчанию.
- Наш пакет `kz.qazmarka.h2k` — `INFO` (включайте точечный `DEBUG` по необходимости).

> На старте Endpoint печатает **одну** строку `INFO` с итоговой конфигурацией логов: путь, `maxFileSize`, `maxBackupIndex`.

---

## Диагностика и эксплуатация

### Быстрая верификация (3 шага)

1. **Пир виден и включён в HBase.**  
   В HBase shell:
   ```HBase shell
   list_peers
   ```
   Убедитесь, что ваш peer в состоянии **ENABLED**, очереди не растут аномально.
2. **Есть события.**  
   Либо выполните тестовый `put` в таблицу c включённым CF, либо дождитесь рабочих апдейтов.
3. **Сообщения долетают в Kafka.**  
   ```bash
   kafka-console-consumer.sh --bootstrap-server <brokers> --topic <topic> --from-beginning --max-messages 5
   ```
   Топик по умолчанию — имя таблицы (см. `h2k.topic.pattern`, по дефолту `${table}`).


**Полезные операции (HBase 1.4.13):**

```HBase shell
# HBase shell
# включить/выключить peer
enable_peer 'kafka_peer_fast'
disable_peer 'kafka_peer_fast'

# показать/изменить ограничения по таблицам/CF
show_peer_tableCFs 'kafka_peer_fast'       # вернёт nil, если ограничений нет
set_peer_tableCFs   'kafka_peer_fast', 'TBL1:cf1;TBL2:cf2,cf3'

# обновить конфиг (например, поменяли acks или bootstrap) — надёжнее через Java API в 1.4.13
rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.updatePeerConfig("kafka_peer_fast", repconf)
```

**JMX/метрики:**

- `Hadoop:service=HBase,name=RegionServer,sub=Replication` — задержки, очереди.
- `kafka.producer:type=producer-metrics,client-id=*` и `...producer-topic-metrics...`.

**Быстрая проверка Kafka:**

```bash
kafka-console-consumer.sh \
  --bootstrap-server 10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092 \
  --topic <ваш_топик> --from-beginning --max-messages 5
```

---

## Типовые ошибки и что посмотреть в логах

- **`NoClassDefFoundError: ...KafkaProducer`** — на RS нет `kafka-clients`.  
  Проверьте `hbase classpath`, при необходимости скопируйте `kafka-clients-2.3.1.jar` и `lz4-java` в `/opt/hbase-default-current/lib/`.
- **`TimeoutException` / `NotEnoughReplicas` / `request timed out`** — проблемы с доставкой в Kafka.  
  Проверьте доступность брокеров, `acks`, ISR; при необходимости увеличьте `h2k.producer.delivery.timeout.ms`, уменьшите `max.in.flight`, включите `idempotence`.
- **`UnknownTopicOrPartition`** — тема не создана.  
  Включите `h2k.ensure.topics=true` или создайте тему вручную; проверьте `h2k.topic.pattern` и фактическое имя.
- **`Schema for table ... not found` (режим `json-phoenix`)** — неверный/отсутствующий `schema.json`.  
  Проверьте `h2k.schema.path`, имя таблицы в ключе (`DEFAULT` без префикса), регистр колонок и типы Phoenix.
- **Пусто в консюмере, но peer ENABLED** — проверьте, что CF включён для репликации (`REPLICATION_SCOPE => 1`) и `h2k.cf.list` содержит нужные CF.  
  Также проверьте фильтры по времени WAL, если включали `h2k.filter.by.wal.ts`.
- **Проблемы с логами/правами** — нет записи в файл.  
  Убедитесь в доступности `${h2k.log.dir}` (или `${hbase.log.dir}`), при старте Endpoint пишет одну строку `INFO` с итоговой конфигурацией логирования.

## «Что тюнить, если…»

- **Пики задержек** — снижайте `linger.ms`; проверьте сеть/GC; при `acks=all` — здоровье ISR/диск.
- **Timeout/NotEnoughReplicas** — увеличьте `delivery.timeout.ms`, уменьшите `max.in.flight`/`batch.size`, проверьте ISR.
- **BufferExhausted** — увеличьте `buffer.memory`, уменьшите `linger.ms`/`batch.size`, включите/усильте `lz4`.

---

## Архитектура (кратко)

```
HBase RegionServer
    └─ WAL edits (WALEntry/WALEdit)
         └─ HBase Replication Framework
              └─ KafkaReplicationEndpoint (init → TopicEnsurer → ProducerPropsFactory)
                   ├─ PayloadBuilder (Decoder/SimpleDecoder|Phoenix)
                   ├─ BatchSender (дозированное ожидание acks)
                   └─ KafkaProducer → Kafka Brokers → <topic per table>
```
*Поток данных и основные узлы; TopicEnsurer отрабатывает только на старте.*

- **KafkaReplicationEndpoint** — группировка `WALEdit` по rowkey без лишних строк; дозированное ожидание ack (минимум блокировок).
- **PayloadBuilder** — стабильный порядок ключей в JSON (`LinkedHashMap`), опциональные метаполя, минимизация копий.
- **Decoder/SimpleDecoder/ValueCodecPhoenix** — без лишних аллокаций; Phoenix‑режим использует `JsonSchemaRegistry`.
- **TopicEnsurer** — безопасное создание тем; backoff на основе `SecureRandom` (без утечек энтропии; не на горячем пути).

**JVM рекомендации:** `-XX:+UseG1GC -XX:MaxGCPauseMillis=50`, `-XX:+AlwaysPreTouch`.

---

## Безопасность

- В коде не используются небезопасные PRNG для целей безопасности в горячем пути.  
  `SecureRandom` применяется только в не‑критичной по производительности части (backoff в TopicEnsurer).
- Логи — русскоязычные, без чувствительных данных (ключи/пароли не пишутся).

---

## Ограничения

- Phoenix PK: поддерживаются **ASC**‑колонки.
- Подключение к Kafka — по умолчанию **PLAINTEXT** (SASL/SSL не настраивается этим компонентом).

---

## FAQ

**Почему нельзя указывать `DEFAULT.TBL_NAME`?**  
В Phoenix таблицы из дефолт-неймспейса указываются как `TBL_NAME` без префикса `DEFAULT.`. Запись `DEFAULT.TBL_NAME` приводит к ошибке парсера.

**LZ4 или Snappy для `compression.type`?**  
Обычно LZ4 быстрее при сопоставимой компрессии, поэтому в профилях FAST/BALANCED используется `lz4`. Для максимальной совместимости и строгих гарантий профиль RELIABLE оставляет `snappy`.

**Где искать логи endpoint?**  
По умолчанию — `${hbase.log.dir}/h2k-endpoint.log`. Можно переопределить `-Dh2k.log.dir`. Ротация управляется `h2k.log.maxFileSize` и `h2k.log.maxBackupIndex`.

**Откуда берётся имя топика, если у меня `h2k.topic.pattern=${table}`?**  
Из `WALEntry` мы берём `namespace` и `qualifier` текущей таблицы. Затем подставляем их в `${table}` по правилу: если `namespace=default` — только `${qualifier}`, иначе `${namespace}_${qualifier}`. Если тема создана вручную, её имя должно совпадать с этим результатом (либо измените шаблон).

---

## Чек‑лист запуска

1. JAR в `/opt/hbase-default-current/lib/`.
2. На RS есть `kafka-clients-2.3.1.jar` и `lz4-java-1.6.0+.jar` (`hbase classpath` это показывает).
3. Создан peer (`fast/balanced/reliable`) с корректным `bootstrap`.
4. Если `json-phoenix` — `schema.json` доступен и путь указан.
5. В логах RS нет ошибок, события появляются в Kafka.

---

## TODO: Реализация варианта через Avro (open-source Schema Registry)

**Статус:** план внедрения. Сейчас endpoint публикует **только JSON (JSONEachRow)**. Всё ниже — дорожная карта. Любые ключи из этого раздела **не работают**, пока соответствующий код не появится в репозитории. Мы придерживаемся правила: **источник истины — фактический исходный код**.

### Зачем Avro

- Бинарный компактный формат → меньше трафика/CPU по сравнению с JSON на объёмах 100M+ событий/день.
- Строгая схема и эволюция полей (backward-compatible изменения).
- С Confluent-совместимым реестром используется «AvroConfluent»: `magic byte (0) + schemaId + Avro-payload`.

### Совместимость стенда

- **Kafka (клиенты/брокеры):** 2.3.1 — совместимы с **Schema Registry 5.3.x** (open-source).
- **Java:** 8 (target 1.8) — ок.
- **HBase 1.4.13 / Phoenix 4.14:** на SR напрямую не влияют.
- **ClickHouse 24.8:** умеет читать `AvroConfluent` из Kafka.

### Узлы QA (в примерах ниже)

- Kafka брокеры: `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`
- Schema Registry (план): порт `8081` на этих же узлах (можно начать с одного, затем 3 для HA).

---

**Совместимость: короткий чек‑лист**

- **JDK:** Schema Registry **5.3.8** работает на Java 8 (1.8). Проверьте `java -version` на всех узлах.
- **Kafka:** брокеры **2.3.1**. `kafkastore.bootstrap.servers` указывает на тот же кластер; топик `_schemas` существует и имеет `replication.factor ≥ 3`.
- **REST‑доступ:** порт **8081** доступен с RegionServer (продьюсеров), ClickHouse и админ‑хостов. Проверка: `curl -s http://&lt;sr-host&gt;:8081/subjects`.
- **Формат для ClickHouse 24.8:** поддерживается `AvroConfluent`; укажите `kafka_format='AvroConfluent'` и `kafka_schema_registry_url` в таблице Kafka‑engine.
- **Безопасность:** используем `PLAINTEXT` в Kafka и Schema Registry. Если в кластере Kafka включены SSL/SASL — потребуется сконфигурировать SR и продьюсеров соответствующим образом.
- **Сериализатор на продьюсере:** `io.confluent.kafka.serializers.KafkaAvroSerializer` и свойство `schema.registry.url` должны быть в конфигурации.
- **Синхронизация времени:** узлы с SR/Kafka/CH синхронизированы по времени (NTP), чтобы исключить аномалии по меткам времени.
- **Альтернативы по версиям:** допускаются SR **5.5.x/6.x**, однако перед апгрейдом обязателен нагрузочный тест на QA и проверка совместимости с Kafka 2.3.1.

### Развёртывание Schema Registry 5.3.x (open-source)

**Ссылки (точные):**
- Исходники (GitHub): <https://github.com/confluentinc/schema-registry>
- Рекомендуемый тег: <https://github.com/confluentinc/schema-registry/tree/v5.3.8> (рекомендуется под Kafka 2.3.1)
- README (сборка/запуск): <https://github.com/confluentinc/schema-registry/blob/v5.3.8/README.md>

_Альтернатива:_ допустимо пробовать Schema Registry **5.5.x/6.x**, но такие версии могут тянуть иные зависимости/минимальные версии JDK/Kafka. Перед апгрейдом обязателен нагрузочный прогон на QA (регистрация/валидация схем, публикация AvroConfluent, потребление в ClickHouse). При отсутствии явной необходимости остаёмся на **5.3.x**.

<a id="avro-compat-checklist"></a>
### Совместимость: короткий чек-лист

- **JDK:** Java 8 (1.8) на всех RegionServer, узлах Schema Registry и ClickHouse.
- **Kafka:** брокеры 2.3.1; системный топик `_schemas` с `replication.factor ≥ 3`; доступность брокеров с узлов SR и RS.
- **Schema Registry:** open-source 5.3.8; REST на `http://<host>:8081`; доступен из RS и ClickHouse; `compatibility.level=BACKWARD` (или ваша политика).
- **ClickHouse:** 24.8; Kafka-engine с `kafka_format='AvroConfluent'` и `kafka_schema_registry_url='http://host1:8081,...'`.
- **Producer (endpoint):** при включении Avro должен использовать `value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer` и `schema.registry.url=...` (в коде пока не реализовано — см. раздел «Изменения в h2k-endpoint (план)»).
- **Безопасность:** единообразие транспорта (PLAINTEXT везде, либо SSL/SASL везде); при SSL/SASL — соответствующие `schema.registry.*`/`kafkastore.*` настройки.
- **Время:** синхронизация NTP/PTP на всех узлах (важно для таймаутов/метрик).
- **Альтернативные SR:** 5.5.x/6.x допустимы, но требуются нагрузочные и длительные (soak) тесты с Kafka 2.3.1 перед продом.

#### Вариант A — tar из Confluent Platform 5.3.x

1. Распакуйте платформу, оставьте только каталог **`schema-registry/`** (со скриптами `bin/schema-registry-start|stop`).
2. Разложите по узлам:
   ```
   /opt/schema-registry/       # bin/, etc/, lib/
   /var/log/schema-registry/   # логи
   ```
3. Конфиг: `/opt/schema-registry/etc/schema-registry/schema-registry.properties`
   ```properties
   listeners=http://0.0.0.0:8081
   host.name=${HOSTNAME}

   # Kafka 2.3.1
   kafkastore.bootstrap.servers=PLAINTEXT://10.254.3.111:9092,PLAINTEXT://10.254.3.112:9092,PLAINTEXT://10.254.3.113:9092

   # Топик для хранения схем (в Kafka)
   kafkastore.topic=_schemas
   kafkastore.topic.replication.factor=3
   kafkastore.timeout.ms=60000

   # Политика совместимости
   compatibility.level=BACKWARD
   ```
4. Юнит systemd: `/etc/systemd/system/schema-registry.service`
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
5. Запуск и проверка:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now schema-registry
   curl -s http://10.254.3.111:8081/subjects   # ожидаем: []
   ```

#### Вариант B — сборка из исходников

> На том же репозитории/теге: <https://github.com/confluentinc/schema-registry/tree/v5.3.6>

```bash
git clone https://github.com/confluentinc/schema-registry.git
cd schema-registry
git checkout v5.3.6
mvn -q -DskipTests package
# далее развернуть как в варианте A (пп. 2–5)
```

---

### Avro-схема: `TBL_JTI_TRACE_CIS_HISTORY`

Конвенции сериализации: `TIMESTAMP → *_ms (epoch-millis, long)`, `UNSIGNED_* → int`, `VARCHAR ARRAY → array<string>`.  
PK: `c` (string), `t` (int), `opd_ms` (long).  
Доп. поля: `event_version_ms` (nullable long), `delete` (boolean, default `false`).

Файл `conf/avro/tbl_jti_trace_cis_history.avsc`:
```json
{
  "type": "record",
  "name": "TBL_JTI_TRACE_CIS_HISTORY_Row",
  "namespace": "kz.qazmarka.h2k",
  "fields": [
    {"name": "c",   "type": "string"},
    {"name": "t",   "type": "int"},
    {"name": "opd_ms", "type": "long"},

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

    {"name": "st",   "type": ["null","int"], "default": null},
    {"name": "ste",  "type": ["null","int"], "default": null},
    {"name": "elr",  "type": ["null","int"], "default": null},

    {"name": "emd_ms", "type": ["null","long"], "default": null},
    {"name": "apd_ms", "type": ["null","long"], "default": null},

    {"name": "p",   "type": ["null","string"], "default": null},
    {"name": "pt",  "type": ["null","int"],    "default": null},
    {"name": "o",   "type": ["null","string"], "default": null},
    {"name": "pn",  "type": ["null","string"], "default": null},
    {"name": "b",   "type": ["null","string"], "default": null},
    {"name": "tt",  "type": ["null","long"],   "default": null},
    {"name": "tm_ms", "type": ["null","long"], "default": null},

    {"name": "ch",  "type": ["null", { "type": "array", "items": "string" }], "default": null},
    {"name": "j",   "type": ["null","string"], "default": null},
    {"name": "pg",  "type": ["null","int"],    "default": null},
    {"name": "et",  "type": ["null","int"],    "default": null},
    {"name": "exd_ms", "type": ["null","long"], "default": null},
    {"name": "pvad","type": ["null","string"], "default": null},
    {"name": "ag",  "type": ["null","string"], "default": null},

    {"name": "event_version_ms", "type": ["null","long"], "default": null},
    {"name": "delete", "type": "boolean", "default": false}
  ]
}
```

**Регистрация схемы** (subject = `<topic>-value`):
```bash
curl -s -X POST http://10.254.3.111:8081/subjects/TBL_JTI_TRACE_CIS_HISTORY-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @conf/avro/tbl_jti_trace_cis_history.avsc
```

---

### ClickHouse 24.8: чтение AvroConfluent

**RAW-таблица (Kafka-engine):**
```sql
CREATE TABLE raw_tbl_jti_trace_cis_history_kafka
(
  c String,
  t Int32,
  opd_ms Int64,

  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),

  st Nullable(Int32), ste Nullable(Int32), elr Nullable(Int32),

  emd_ms Nullable(Int64), apd_ms Nullable(Int64),

  p Nullable(String), pt Nullable(Int32), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm_ms Nullable(Int64),

  ch Array(String), j Nullable(String), pg Nullable(Int32), et Nullable(Int32),
  exd_ms Nullable(Int64), pvad Nullable(String), ag Nullable(String),

  event_version_ms Nullable(Int64),
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
```

**Целевая MergeTree + MV:**
```sql
CREATE TABLE tbl_jti_trace_cis_history
(
  c String,
  t UInt8,
  opd_ms       DateTime64(3),
  event_version_ms Nullable(DateTime64(3)),
  `delete` UInt8,

  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),

  st Nullable(UInt8), ste Nullable(UInt8), elr Nullable(UInt8),

  emd_ms Nullable(DateTime64(3)), apd_ms Nullable(DateTime64(3)),

  p Nullable(String), pt Nullable(UInt8), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm_ms Nullable(DateTime64(3)),

  ch Array(String), j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8),
  exd_ms Nullable(DateTime64(3)), pvad Nullable(String), ag Nullable(String)
)
ENGINE = MergeTree
ORDER BY (c, t, opd_ms);

CREATE MATERIALIZED VIEW mv_tbl_jti_trace_cis_history
TO tbl_jti_trace_cis_history AS
SELECT
  c,
  CAST(t AS UInt8) AS t,
  toDateTime64(opd_ms/1000, 3) AS opd_ms,
  ifNull(toDateTime64(event_version_ms/1000, 3), NULL) AS event_version_ms,
  `delete`,
  id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
  CAST(st  AS Nullable(UInt8))  AS st,
  CAST(ste AS Nullable(UInt8))  AS ste,
  CAST(elr AS Nullable(UInt8))  AS elr,
  ifNull(toDateTime64(emd_ms/1000, 3), NULL) AS emd_ms,
  ifNull(toDateTime64(apd_ms/1000, 3), NULL) AS apd_ms,
  p,
  CAST(pt AS Nullable(UInt8)) AS pt,
  o, pn, b,
  tt,
  ifNull(toDateTime64(tm_ms/1000, 3), NULL)  AS tm_ms,
  ch, j,
  CAST(pg AS Nullable(UInt16)) AS pg,
  CAST(et AS Nullable(UInt8))  AS et,
  ifNull(toDateTime64(exd_ms/1000, 3), NULL) AS exd_ms,
  pvad, ag
FROM raw_tbl_jti_trace_cis_history_kafka;
```

---

### Изменения в h2k-endpoint (план; не реализовано)

1. Новый ключ (дефолт делаем **avro**):
   ```properties
   h2k.output.format = json | avro   # дефолт: avro
   ```
   Пока в репозитории нет поддержки Avro — фактический формат остаётся JSON.
2. Для `avro` добавить:
   ```properties
   h2k.schema.registry.urls = http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081
   h2k.schema.subject.pattern = ${topic}-value
   # В KafkaProducer:
   value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
   schema.registry.url=<urls>
   ```
3. В `KafkaReplicationEndpoint` собирать Avro-record из уже типизированной карты (как сейчас для JSON в режиме Phoenix).
4. Тесты: `MockSchemaRegistryClient`, проверка совместимости, негативные кейсы.

---

### Порядок включения (минимум)

1. Поднять Schema Registry 5.3.x на QA; убедиться, что `/subjects` отвечает.
2. Согласовать и зарегистрировать Avro-схемы для нужных топиков.
3. Реализовать поддержку `h2k.output.format=avro` в коде, собрать JAR.
4. Включить Avro для `TBL_JTI_TRACE_CIS_HISTORY`, проверить доставку в Kafka.
5. Завести RAW-таблицу и MV в ClickHouse; проверить лаг и типы.
6. Провести нагрузочные тесты, затем расширять перечень таблиц.

#### Коротко: проверьте перед стартом Avro

- Java 8, Kafka 2.3.1, SR 5.3.8 REST:8081 доступны.
- В ClickHouse настроен `AvroConfluent` + `kafka_schema_registry_url`.
- В endpoint будет включён Avro-serializer и `schema.registry.url` (после реализации).
- Транспорт единообразный (PLAINTEXT/SSL/SASL), время синхронизировано.
- Под альтернативные SR версии проведён нагрузочный тест.  
  См. полный чек-лист: [ссылка](#avro-compat-checklist).