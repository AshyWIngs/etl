# HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (JSONEachRow)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Класс Endpoint:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x, публикующий изменения строк в Kafka как **одну JSON‑строку на событие** (формат JSONEachRow). Код и конфиги оптимизированы под минимальные аллокации и высокую пропускную способность.

---

## Поддерживаемые версии
- **Java**: 8 (target 1.8)
- **HBase**: 1.4.13 (совместимо с 1.4.x)
- **Kafka**: 2.3.1 (совместимые клиенты)
- **Phoenix**: совместимая с RS версия `phoenix-core` (если используется режим декодирования `json-phoenix`)

---

## Сборка
```bash
mvn -q -DskipTests clean package
# Артефакт: target/h2k-endpoint-0.1.0.jar
```

---

## Деплой
1. Скопировать JAR на **все RegionServer** в каталог `/opt/hbase-default-current/lib/`.
2. Перезапустить RegionServer.

> Если кладёте JAR не в стандартный `.../lib`, убедитесь, что путь добавлен в `HBASE_CLASSPATH` у RS.

---

## Конфигурация

- Базовые фрагменты:
  - `conf/hbase-site.xml` — системные ключи (RS).
  - `conf/add_peer_shell_fast.txt` — пример создания peer (максимальная скорость).
  - `conf/add_peer_shell_balanced.txt` — пример создания peer (сбалансированный профиль).
  - `conf/add_peer_shell_reliable.txt` — пример создания peer (надёжный профиль).
- Все ключи читаются из объединённой конфигурации (site + peer). **Приоритет у peer**.

**Размещение файлов в проде:**
- JAR: `/opt/hbase-default-current/lib/`
- Конфиги HBase: `/opt/hbase-default-current/conf/` (не перезаписываем `hbase-site.xml`, только добавляем нужные ключи).
- `schema.json`: положите на все RS (например, `/etc/h2k/schema.json`) и укажите путь через `h2k.schema.path`.

### Ключи `h2k.*`

**Обязательные:**
- `h2k.kafka.bootstrap.servers` — брокеры Kafka, например: `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`.

**Рекомендуемые (рантайм):**
- `h2k.topic.pattern` — шаблон топика, по умолчанию `${table}`. Доступны плейсхолдеры: `${table}` (`namespace_qualifier`), `${namespace}`, `${qualifier}`.
- `h2k.cf` — рабочее ColumnFamily (по умолчанию `0`).
- `h2k.salted` / `h2k.salt.bytes` — если в rowkey используется SALT.
- `h2k.decode.mode` — `simple` или `json-phoenix`.
- `h2k.schema.path` — путь к `schema.json` (обязательно при `json-phoenix`).
- `h2k.json.serialize.nulls` — сериализовать `null` в JSON (по умолчанию `false`).
- `h2k.producer.await.every` — как часто ждать подтверждений от Kafka (по умолчанию `500`).
- `h2k.producer.await.timeout.ms` — общий таймаут ожидания подтверждений (по умолчанию `180000`).

**Kafka Producer (pass‑through)** — любые ключи с префиксом `h2k.producer.*` передаются в продьюсер:
- `h2k.producer.acks` (`0`/`1`/`all`)
- `h2k.producer.enable.idempotence` (`true`/`false`)
- `h2k.producer.max.in.flight` (реком. `1` для строгого порядка, `2..5` для скорости)
- `h2k.producer.linger.ms`, `h2k.producer.batch.size`, `h2k.producer.compression.type`
- `h2k.producer.retries`, `h2k.producer.delivery.timeout.ms`, `h2k.producer.request.timeout.ms`
- `h2k.producer.buffer.memory`, `h2k.producer.client.id`, `h2k.producer.max.block.ms`
- и любые другие валидные ключи Kafka *без* префикса (мы их прокидываем как есть).

> Полные рабочие примеры значений смотрите в `conf/add_peer_shell_fast.txt`, `conf/add_peer_shell_balanced.txt`, `conf/add_peer_shell_reliable.txt`.

**Сетевые и безопасность (по умолчанию):**
- Подключение **PLAINTEXT**, без SASL/SSL (Kafka), без аутентификации HBase/ZK.
- Для защищённых сред настройка авторизации/шифрования не входит в данный README и потребует отдельных ключей (SASL/SCRAM, SSL, ACL в HBase/ZK).

---

### Профили подключения peer
- **fast** — максимальная скорость, возможна потеря записей при авариях: `conf/add_peer_shell_fast.txt`
- **balanced** — компромисс скорость/надёжность: `conf/add_peer_shell_balanced.txt`
- **reliable** — строгие гарантии доставки и порядка, ниже throughput: `conf/add_peer_shell_reliable.txt`

---

## Пример add_peer (краткий)
Ниже — укороченный пример через Java‑API из `hbase shell` (подмените ZK/пути под ваш кластер):
```bash
repconf = org.apache.hadoop.hbase.replication.ReplicationPeerConfig.new
repconf.setClusterKey("10.254.3.111,10.254.3.112,10.254.3.113:2181:/hbase")
repconf.setReplicationEndpointImpl("kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint")

java_conf = repconf.getConfiguration
java_conf.put("h2k.kafka.bootstrap.servers", "10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092")
java_conf.put("h2k.topic.pattern", "${table}")
java_conf.put("h2k.cf", "0")
java_conf.put("h2k.decode.mode", "json-phoenix")
java_conf.put("h2k.schema.path", "/etc/h2k/schema.json")
java_conf.put("h2k.producer.acks", "all")
java_conf.put("h2k.producer.enable.idempotence", "true")
java_conf.put("h2k.producer.max.in.flight", "1")
java_conf.put("h2k.producer.linger.ms", "50")
java_conf.put("h2k.producer.batch.size", "65536")
java_conf.put("h2k.producer.compression.type", "snappy")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer", repconf, java.util.HashMap.new)
```

---

## Формат сообщения в Kafka (JSONEachRow)
Endpoint формирует **одну JSON‑строку на rowkey**. Структура соответствует текущей логике:
```json
{
  "c": "<pk_part_c>",
  "t": 0,
  "opd_ms": 0,
  "event_version": 0,
  "delete": false,
  "<qualifier1>": <value1>,
  "<qualifier2>": <value2>
}
```
- `c`, `t`, `opd_ms` — части PK, декодированные из `rowkey` Phoenix (`VARCHAR`, `UNSIGNED_TINYINT`, `TIMESTAMP` в миллисекундах).
- `event_version` — максимальный `timestamp` среди ячеек выбранного CF для данной строки.
- `delete` — присутствует и равен `true`, если пришёл delete‑маркер по нашему CF.
- Остальные пары `qualifier → value` добавляются после декодирования байт через выбранный `Decoder`.

> Конкретный набор колонок и их типы определяется `schema.json` (при `h2k.decode.mode=json-phoenix`).

---

## Схема (`schema.json`) и `JsonSchemaRegistry`
- Формат:
  ```json
  {
    "NAMESPACE:TBL_NAME": {
      "columns": {
        "id": "VARCHAR",
        "created_at": "TIMESTAMP",
        "flags": "UNSIGNED_INT"
      }
    }
  }
  ```
- Для **имён таблиц** публикуются алиасы: исходное, `UPPER`, `lower`, а также «короткое» имя после `:` (и его `UPPER`/`lower`).
- Для **имён колонок** — также `orig/UPPER/lower`.
- Имена **типов** канонизируются в `UPPER` (`Locale.ROOT`). Неизвестные типы допускаются с предупреждением.
- После загрузки схема **иммутабельна**; быстрая перезагрузка — `refresh()` (или рестарт RS).

---

## Архитектура и производительность
- **KafkaReplicationEndpoint**
  - Группировка `WALEdit` по rowkey без промежуточных `String`.
  - Нулевые копии на горячем пути; ровно одна копия `rowkey` на запись (ключ Kafka + декодирование PK).
  - Очереди `Future<RecordMetadata>` с дозированным ожиданием ack (`h2k.producer.await.every`).
- **PhoenixRowKeyDecoder** — быстрый парсер PK (`c`,`t`,`opd_ms`) с поддержкой `SALT` (только ASC‑колонки в PK).
- **JsonSchemaRegistry** — иммутабельный снимок схемы + кэш `TableName → columns`.
- **Decoder/SimpleDecoder/ValueCodecPhoenix** — единый интерфейс, быстрые перегрузки без лишних аллокаций.

**Рекомендации JVM** для high‑load:
- `-XX:+UseG1GC -XX:MaxGCPauseMillis=50`
- `-XX:+AlwaysPreTouch` при больших heap
- Уровень логов `INFO/WARN` на проде; `DEBUG` только точечно.

**Тюнинг под большой объём (100M+ строк/сутки):**
- Начните с профиля **fast**; если фиксируете потери при авариях — переходите на **balanced**/**reliable**.
- Подбирайте `h2k.producer.linger.ms` и `h2k.producer.batch.size` под сеть/CPU.
- Для упора в сеть увеличивайте `h2k.producer.compression.type=snappy`/`lz4`.
- Следите за `buffer.memory`; при `BufferExhaustedException` увеличьте `buffer.memory` или снижайте `linger.ms`.

---

## Диагностика и отладка

**Проверка состояния репликации (HBase shell):**
```bash
status 'replication'
list_peers
```

**Логи на RS:**
- Инициализация endpoint:
  ```
  INFO  KafkaReplicationEndpoint initialized: topicPattern=${table}, cf=0, salted=false, saltBytes=1
  INFO  Decode mode=json-phoenix, schema=/etc/h2k/schema.json
  ```
- Ошибки/предупреждения по схеме:
  ```
  WARN  Схема пуста или имеет неожиданный формат — реестр останется пустым
  WARN  Обнаружен дубликат таблицы '...' (ключ '...') в файле схемы '...' — существующее определение будет перезаписано
  ```
- Ошибки Kafka (например, таймаут ожидания ack):
  ```
  ERROR replicate() timed out while waiting on Kafka futures
  ```

**Метрики / JMX:**
- На RegionServer поднимите JMX и проверьте:
  - `Hadoop:service=HBase,name=RegionServer,sub=Replication` — задержки/очереди.
- На стороне приложения Kafka‑клиента:
  - `kafka.producer:type=producer-metrics,client-id=*`
  - `kafka.producer:type=producer-topic-metrics,client-id=*,topic=*`

**Быстрая проверка в Kafka:**
```bash
kafka-console-consumer.sh \
  --bootstrap-server 10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092 \
  --topic <ваш_топик> --from-beginning --max-messages 5
```

**Тонкие подсказки по тюнингу:**
- **Пики задержек**: уменьшите `h2k.producer.linger.ms`, проверьте сеть и GC; при `acks=all` — здоровье ISR и диски брокеров.
- **Ошибки брокера** (`TimeoutException`, `NotEnoughReplicas`): проверьте ISR/кворум, увеличьте `delivery.timeout.ms`, понизьте `max.in.flight`.
- **Переполнения буфера** (`BufferExhaustedException`): увеличьте `buffer.memory`, снижайте `linger.ms`/`batch.size`, включите компрессию.

Больше примеров и подсказок см. в профилях `conf/add_peer_shell_*.txt` (блоки «Проверка/отладка» и «Быстрые подсказки по тюнингу»).

---

## Быстрый чек‑лист запуска
1. `schema.json` доступен всем RS по `h2k.schema.path`.
2. Peer создан и указывает на `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`.
3. В peer заданы `h2k.kafka.bootstrap.servers` и выбран профиль продьюсера (`fast`/`balanced`/`reliable`).
4. В логах RS нет ошибок загрузки схемы.
5. В целевом Kafka‑топике появляются JSON‑события.

---

## Ограничения
- Поддержан парсинг Phoenix PK **только для ASC‑колонок** в ключе.
- Подключение к Kafka по умолчанию **PLAINTEXT** (без аутентификации/шифрования).

---

## Лицензия
См. `LICENSE` (если присутствует в репозитории).