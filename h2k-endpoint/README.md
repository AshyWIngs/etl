# HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (JSONEachRow)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Endpoint‑класс:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x, публикующий изменения строк в Kafka как **одну JSON‑строку на событие** (формат JSONEachRow). Код и конфиги ориентированы на минимальные аллокации и высокую пропускную способность, с приоритетом стабильности.

---

## Быстрый старт

1) **Соберите и разложите JAR** на все RegionServer:
   ```bash
   mvn -q -DskipTests clean package
   cp target/h2k-endpoint-*.jar /opt/hbase-default-current/lib/
   ```
2) **Подготовьте схему Phoenix** (если используете `json-phoenix`), файл:
   `/opt/hbase-default-current/conf/schema.json` (см. раздел «Схема Phoenix»).
3) **Включите репликацию CF** в нужных таблицах (пример для `TBL_JTI_TRACE_CIS_HISTORY`, CF `d`):
   Также убедитесь, что в глобальном `hbase-site.xml` включена репликация кластера:
   ```
   <property><name>hbase.replication</name><value>true</value></property>
   ```

   ```
   # HBase shell
   disable 'TBL_JTI_TRACE_CIS_HISTORY'
   alter  'TBL_JTI_TRACE_CIS_HISTORY', { NAME => 'd', REPLICATION_SCOPE => 1 }
   enable 'TBL_JTI_TRACE_CIS_HISTORY'
   ```
4) **Создайте peer** (выберите профиль ниже: `FAST` / `BALANCED` / `RELIABLE`). Минимальный набор ключей:
   - `h2k.kafka.bootstrap.servers`
   - `h2k.cf.list` (например, `d`)
   - `h2k.decode.mode=json-phoenix`
   - `h2k.schema.path=/opt/hbase-default-current/conf/schema.json`
   - `h2k.salt.map=TBL_JTI_TRACE_CIS_HISTORY=1`
   - `h2k.capacity.hints=TBL_JTI_TRACE_CIS_HISTORY=32`
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
   - `kafka-clients-2.3.1.jar`  
   - `lz4-java-1.6.0+.jar`  
   - `snappy-java-1.1.x+.jar`  (если используете `compression.type=snappy`)
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


### TL;DR: минимально достаточно для запуска

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

### Обязательные

- `h2k.kafka.bootstrap.servers` — брокеры Kafka, напр.:  
  `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`
- `h2k.cf.list` — список CF для экспорта (например: `d`, `b`, `0`; несколько через запятую).
- `h2k.salt.map` — карта «таблица → длина префикса соли в байтах». Для Phoenix‑salted таблиц (`SALT_BUCKETS>0`) значение **всегда `1`**. Пример: `TBL_JTI_TRACE_CIS_HISTORY=1`. Обязательно указывать для всех salted‑таблиц, чтобы корректно работать с rowkey.
- `h2k.capacity.hints` — карта «таблица → ожидаемое число ключей в корневом JSON». Помогает заранее выделить ёмкость `LinkedHashMap` и избежать расширений. Пример: `TBL_JTI_TRACE_CIS_HISTORY=32`. В продакшене настоятельно рекомендуем указывать.
- Для режима json-phoenix дополнительно обязательны: h2k.decode.mode=json-phoenix и h2k.schema.path=/opt/hbase-default-current/conf/schema.json.

### Поведение Endpoint/Decoder

- `h2k.topic.pattern` — шаблон топика; по умолчанию `${table}`.  
  Плейсхолдеры: `${table}` (=`namespace_qualifier`), `${namespace}`, `${qualifier}`.
- `h2k.cf.list` — какие ColumnFamily экспортировать: список через запятую, напр.: `d,b,0`.  
  Для CF в `DEFAULT`‑неймспейсе просто имя CF; для других таблиц CF указываются так же, как в `describe`.
- `h2k.decode.mode` — `simple` или `json-phoenix`.
- `h2k.schema.path` — путь к `schema.json` (обязателен при `json-phoenix`).
- `h2k.json.serialize.nulls` — сериализовать `null` в JSON (по умолчанию `false`).
- `h2k.include.meta` — включать служебные поля `_table/_namespace/_qualifier/_cf/_cells_total/_cells_cf/event_version/delete` (по умолчанию `false`).
- `h2k.include.meta.wal` — включать `_wal_seq/_wal_write_time` (по умолчанию `false`).
- `h2k.include.rowkey` — включать rowkey в JSON (по умолчанию `false`).
- `h2k.rowkey.encoding` — формат rowkey при включении `include.rowkey`: `BASE64` (по умолчанию) или `HEX`.
- `h2k.filter.by.wal.ts` — включить фильтрацию по минимальному WAL‑timestamp (по умолчанию `false`).
- `h2k.wal.min.ts` — минимальный `timestamp` (epoch ms), учитывается при `filter.by.wal.ts=true`.
- `h2k.salt.map` — карта «таблица → длина префикса соли в байтах». Для Phoenix‑salted таблиц значение всегда `1`. Пример: `TBL_JTI_TRACE_CIS_HISTORY=1,AGG.INC_DOCS_ACT=1`.
- `h2k.capacity.hints` — карта «таблица → подсказка ёмкости корневого JSON‑объекта». Пример: `TBL_JTI_TRACE_CIS_HISTORY=32`.

### Параметры дозированного ожидания ACK (BatchSender)

- `h2k.producer.await.every` — ждать подтверждения каждые **N** отправок (по умолчанию `500`).
- `h2k.producer.await.timeout.ms` — общий таймаут ожидания всех futures, мс (по умолчанию `180000`).
- `h2k.producer.batch.counters.enabled` — лёгкие счётчики для диагностики (по умолчанию `false`).
- `h2k.producer.batch.debug.on.failure` — подробности авто‑сброса в DEBUG (по умолчанию `false`).

### Kafka Producer (pass‑through)

Любые ключи с префиксом `h2k.producer.*` прокидываются в `KafkaProducer` как нативные. Важные:

- `h2k.producer.acks` — `0` / `1` / `all`
- `h2k.producer.enable.idempotence` — `true` / `false`
- `h2k.producer.max.in.flight` — `1` для строгого порядка; `2..5` быстрее
- `h2k.producer.linger.ms`, `h2k.producer.batch.size`
- `h2k.producer.compression.type` — **рекомендуем `lz4`**
- `h2k.producer.delivery.timeout.ms`, `retries`, `request.timeout.ms`, `buffer.memory`, `client.id`, ...

Остальные ключи `h2k.producer.*` прокидываются в `KafkaProducer` без переименования.

> **Про `client.id`:** **не задавайте фиксированное значение**. Endpoint сам сформирует уникальный `client.id` на каждой ноде (по `hostname`, фолбэк — `UUID`), чтобы исключить конфликты и упростить метрики. Если всё‑таки задаёте вручную — обеспечьте уникальность на ноду.

### Автосоздание топиков (опционально)

- `h2k.ensure.topics` — включить проверку/создание тем при старте (по умолчанию `true`).
- `h2k.topic.partitions` / `h2k.topic.replication` — параметры при создании темы.
- `h2k.topic.config.*` — pass‑through‑свойства темы (`retention.ms`, `cleanup.policy`, ...).
- `h2k.admin.timeout.ms` — таймаут операций AdminClient (по умолчанию `30000`).
- `h2k.ensure.unknown.backoff.ms` — короткий backoff на не‑фатальные ошибки (по умолчанию `5000`).

---

## Таблица ключей (сводно)

| Ключ | Дефолт | Единицы | Где применяется | Назначение / примечание |
|---|---|---|---|---|
| `h2k.kafka.bootstrap.servers` | — | `host:port` через запятую | KafkaReplicationEndpoint → KafkaProducer | Список брокеров Kafka |
| `h2k.topic.pattern` | `${table}` | шаблон | KafkaReplicationEndpoint | Шаблон имени топика. Плейсхолдеры: `${table}` (= `namespace_qualifier`), `${namespace}`, `${qualifier}` |
| `h2k.cf.list` | — | CSV | PayloadBuilder | Список CF для экспорта (`d,b,0`). Не существующие CF игнорируются без ошибок |
| `h2k.decode.mode` | `simple` | enum | KafkaReplicationEndpoint | Режим декодирования: `simple` или `json-phoenix` |
| `h2k.schema.path` | — | путь | JsonSchemaRegistry | Путь к единственному файлу `schema.json`. Используется только в режиме `json-phoenix`. |
| `h2k.json.serialize.nulls` | `false` | boolean | Gson в Endpoint | Добавлять ли `null` в JSON |
| `h2k.include.meta` | `false` | boolean | PayloadBuilder | Добавлять служебные поля (+8 ключей) |
| `h2k.include.meta.wal` | `false` | boolean | PayloadBuilder | Добавлять WAL‑метаданные (+2 ключа) |
| `h2k.include.rowkey` | `false` | boolean | PayloadBuilder | Включать rowkey (+1 ключ) |
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

### Устаревшие ключи

- `h2k.rowkey.base64` — не используется. Вместо него применяйте `h2k.rowkey.encoding=BASE64|HEX`.

---

## Подсказки ёмкости и метаданные (как выбрать значения)

**Зачем нужны `h2k.capacity.hints`:** PayloadBuilder создаёт корневой `LinkedHashMap` с заранее рассчитанной ёмкостью, чтобы **избежать внутренних расширений** и лишних копирований. Это заметно снижает нагрузку GC на больших сообщениях.

**Как считать hint для таблицы:**  
Возьмите «типичный максимум не‑`null` полей» в ваших данных **по выбранным CF**, и **если включены мета‑поля**, добавьте:

- если `h2k.include.meta=true` → `+8` ключей: `_table,_namespace,_qualifier,_cf,_cells_total,_cells_cf,event_version,delete`;
- если `h2k.include.meta.wal=true` → `+2` ключа: `_wal_seq,_wal_write_time`;
- если `h2k.include.rowkey=true` → `+1` ключ: `rowkey` (`hex` или `base64`).

**Пример (ваш прод):**  
Таблица `TBL_JTI_TRACE_CIS_HISTORY` имеет 32 логических поля.  
При текущих настройках (`h2k.include.meta=false`, `h2k.include.meta.wal=false`, `h2k.include.rowkey=false`) разумный hint — **`32`**.  
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

```
# HBase shell
# Пример: TBL_JTI_TRACE_CIS_HISTORY, включаем CF 'd'
disable 'TBL_JTI_TRACE_CIS_HISTORY'
alter  'TBL_JTI_TRACE_CIS_HISTORY', { NAME => 'd', REPLICATION_SCOPE => 1 }
enable 'TBL_JTI_TRACE_CIS_HISTORY'
```

```
# HBase shell
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

## Профили peer (готовые команды)

Подставьте ваши ZK/пути.  
**ZK:** `10.254.3.111,10.254.3.112,10.254.3.113:2181:/hbase`  
**Kafka:** `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`

### 1) FAST — максимальная скорость

```
# HBase shell
repconf = org.apache.hadoop.hbase.replication.ReplicationPeerConfig.new
repconf.setClusterKey("10.254.3.111,10.254.3.112,10.254.3.113:2181:/hbase")
repconf.setReplicationEndpointImpl("kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint")

java_conf = repconf.getConfiguration
java_conf.put("h2k.kafka.bootstrap.servers", "10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092")
java_conf.put("h2k.topic.pattern", "${table}")
java_conf.put("h2k.cf.list", "d")
java_conf.put("h2k.decode.mode", "json-phoenix")
java_conf.put("h2k.schema.path", "/opt/hbase-default-current/conf/schema.json")
java_conf.put("h2k.salt.map", "TBL_JTI_TRACE_CIS_HISTORY=1")
java_conf.put("h2k.capacity.hints", "TBL_JTI_TRACE_CIS_HISTORY=32")

# скорость > надёжность
java_conf.put("h2k.producer.acks", "1")
java_conf.put("h2k.producer.enable.idempotence", "false")
java_conf.put("h2k.producer.max.in.flight", "5")
java_conf.put("h2k.producer.linger.ms", "100")
java_conf.put("h2k.producer.batch.size", "524288")       # 512 KiB
java_conf.put("h2k.producer.compression.type", "lz4")
java_conf.put("h2k.producer.retries", "10")
java_conf.put("h2k.producer.request.timeout.ms", "30000")
java_conf.put("h2k.producer.delivery.timeout.ms", "90000")
java_conf.put("h2k.producer.buffer.memory", "268435456")  # 256 MiB
java_conf.put("h2k.producer.max.request.size", "2097152") # 2 MiB
java_conf.put("h2k.producer.await.every", "500")
java_conf.put("h2k.producer.await.timeout.ms", "180000")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer_fast", repconf, java.util.HashMap.new)
```

### 2) BALANCED — компромисс

```
# HBase shell
repconf = org.apache.hadoop.hbase.replication.ReplicationPeerConfig.new
repconf.setClusterKey("10.254.3.111,10.254.3.112,10.254.3.113:2181:/hbase")
repconf.setReplicationEndpointImpl("kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint")

java_conf = repconf.getConfiguration
java_conf.put("h2k.kafka.bootstrap.servers", "10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092")
java_conf.put("h2k.topic.pattern", "${table}")
java_conf.put("h2k.cf.list", "d")
java_conf.put("h2k.decode.mode", "json-phoenix")
java_conf.put("h2k.schema.path", "/opt/hbase-default-current/conf/schema.json")
java_conf.put("h2k.salt.map", "TBL_JTI_TRACE_CIS_HISTORY=1")
java_conf.put("h2k.capacity.hints", "TBL_JTI_TRACE_CIS_HISTORY=32")

# компромисс скорость/надёжность
java_conf.put("h2k.producer.acks", "all")
java_conf.put("h2k.producer.enable.idempotence", "true")
java_conf.put("h2k.producer.max.in.flight", "3")
java_conf.put("h2k.producer.linger.ms", "100")
java_conf.put("h2k.producer.batch.size", "131072")        # 128 KiB
java_conf.put("h2k.producer.compression.type", "lz4")
java_conf.put("h2k.producer.retries", "2147483647")
java_conf.put("h2k.producer.request.timeout.ms", "60000")
java_conf.put("h2k.producer.delivery.timeout.ms", "300000")
java_conf.put("h2k.producer.buffer.memory", "268435456")  # 256 MiB
java_conf.put("h2k.producer.max.request.size", "2097152") # 2 MiB
java_conf.put("h2k.producer.await.every", "500")
java_conf.put("h2k.producer.await.timeout.ms", "300000")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer_balanced", repconf, java.util.HashMap.new)
```

### 3) RELIABLE — строгие гарантии

```
# HBase shell
repconf = org.apache.hadoop.hbase.replication.ReplicationPeerConfig.new
repconf.setClusterKey("10.254.3.111,10.254.3.112,10.254.3.113:2181:/hbase")
repconf.setReplicationEndpointImpl("kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint")

java_conf = repconf.getConfiguration
java_conf.put("h2k.kafka.bootstrap.servers", "10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092")
java_conf.put("h2k.topic.pattern", "${table}")
java_conf.put("h2k.cf.list", "d")
java_conf.put("h2k.decode.mode", "json-phoenix")
java_conf.put("h2k.schema.path", "/opt/hbase-default-current/conf/schema.json")
java_conf.put("h2k.salt.map", "TBL_JTI_TRACE_CIS_HISTORY=1")
java_conf.put("h2k.capacity.hints", "TBL_JTI_TRACE_CIS_HISTORY=32")

# приоритет — порядок и отсутствие дублей
java_conf.put("h2k.producer.acks", "all")
java_conf.put("h2k.producer.enable.idempotence", "true")
java_conf.put("h2k.producer.max.in.flight", "1")
java_conf.put("h2k.producer.linger.ms", "50")
java_conf.put("h2k.producer.batch.size", "65536")         # 64 KiB
java_conf.put("h2k.producer.compression.type", "snappy")
java_conf.put("h2k.producer.retries", "2147483647")
java_conf.put("h2k.producer.request.timeout.ms", "120000")
java_conf.put("h2k.producer.delivery.timeout.ms", "300000")
java_conf.put("h2k.producer.buffer.memory", "268435456")  # 256 MiB
java_conf.put("h2k.producer.max.request.size", "2097152") # 2 MiB
java_conf.put("h2k.producer.await.every", "500")
java_conf.put("h2k.producer.await.timeout.ms", "300000")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer_reliable", repconf, java.util.HashMap.new)
```
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
   ```
   # HBase shell
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

**Проверка репликации (HBase shell):**

```
# HBase shell
list_peers
```

**Полезные операции:**

```
# HBase shell
# включить/выключить peer
enable_peer 'kafka_peer_fast'
disable_peer 'kafka_peer_fast'

# обновить конфиг (например, поменяли acks или bootstrap) — через Java API в 1.4.13
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
В Phoenix для таблиц из дефолт‑неймспейса используется просто `TBL_NAME` без префикса `DEFAULT.`. Указание `DEFAULT.TBL_NAME` приводит к `ERROR 601 (42P00) Syntax error` или `ERROR 1012 (42M03) Table undefined`.

**Почему `scope=provided` в POM?**  
Чтобы не тащить в наш JAR то, что уже присутствует на RS (HBase/Hadoop/Phoenix/Kafka). Это снижает риски конфликтов и размер артефакта.

**Snappy или LZ4?**  
Для нашей нагрузки **LZ4** обычно выигрывает (ниже CPU/latency при сравнимой компрессии), поэтому примеры используют `lz4`.

**Где лежат логи?**  
По умолчанию — `${hbase.log.dir}` (как у HBase). Можно переопределить `-Dh2k.log.dir`. Имя файла: `h2k-endpoint.log`.

---

## Чек‑лист запуска

1. JAR в `/opt/hbase-default-current/lib/`.
2. На RS есть `kafka-clients-2.3.1.jar` и `lz4-java-1.6.0+.jar` (`hbase classpath` это показывает).
3. Создан peer (`fast/balanced/reliable`) с корректным `bootstrap`.
4. Если `json-phoenix` — `schema.json` доступен и путь указан.
5. В логах RS нет ошибок, события появляются в Kafka.

---

## Лицензия

См. `LICENSE` (если присутствует).