# HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (JSONEachRow)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Класс Endpoint:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x, публикующий изменения строк в Kafka как **одну JSON‑строку на событие** (формат JSONEachRow). Код и конфиги оптимизированы под минимальные аллокации и высокую пропускную способность.

---

## Поддерживаемые версии
- **Java**: 8 (target 1.8)
- **HBase**: 1.4.13 (совместимо с 1.4.x)
- **Kafka**: 2.3.1 (клиенты)
- **Phoenix**: 4.14/4.15 для HBase‑1.4 (опционально, для режима `json-phoenix`)

---

## Сборка
```bash
mvn -q -DskipTests clean package
# Артефакт: target/h2k-endpoint-0.1.0.jar
```

---

## Деплой
1. Скопировать JAR на **все RegionServer** в каталог **`/opt/hbase-default-current/lib/`**.  
   > Если используете другой путь — добавьте его в `HBASE_CLASSPATH`.
2. Перезапустить RegionServer.

---

## Конфигурация (где хранить ключи)

- Используем **оба места**: системный `hbase-site.xml` и конфиг **peer**.  
  Ключи `h2k.*` можно класть в любом из них; **приоритет у peer**.
- **Best‑practice:** не заменять существующий `hbase-site.xml`, а **добавлять** наши ключи (PR/Change‑mgmt проще).
- Файлы:
  - `conf/hbase-site.xml` — системные ключи RS (минимум).
  - `conf/add_peer_shell_fast.txt` / `balanced` / `reliable` — примеры создания peer с параметрами.
  - `schema.json` — если включён `json-phoenix` (раскладываем в одинаковый путь на все RS, например `/etc/h2k/schema.json`).

**Размещение в проде:**
- JAR: `/opt/hbase-default-current/lib/`
- HBase‑конфиги: `/opt/hbase-default-current/conf/`
- `schema.json`: `/etc/h2k/schema.json` (пример) + `h2k.schema.path=/etc/h2k/schema.json`

---

## Ключи `h2k.*`

### Обязательные
- `h2k.kafka.bootstrap.servers` — брокеры Kafka, например:  
  `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`

### Рантайм
- `h2k.topic.pattern` — шаблон топика; по умолчанию `${table}`. Плейсхолдеры: `${table}` (`namespace_qualifier`), `${namespace}`, `${qualifier}`.
- `h2k.cf` — ColumnFamily для экспорта (по умолчанию `0`).
- `h2k.salted` / `h2k.salt.bytes` — SALT в rowkey (если используется).
- `h2k.decode.mode` — `simple` или `json-phoenix`.
- `h2k.schema.path` — путь к `schema.json` (обязателен при `json-phoenix`).
- `h2k.json.serialize.nulls` — сериализовать `null` (по умолчанию `false`).
- `h2k.producer.await.every` — ждать подтверждений каждые N сообщений (по умолчанию `500`).
- `h2k.producer.await.timeout.ms` — общий таймаут ожидания ack (по умолчанию `180000`).

**Диагностика BatchSender (опционально):**
- `h2k.producer.batch.counters.enabled` — включить «лёгкие» внутренние счётчики; читается при старте, на горячий путь не влияет (по умолчанию `false`).
- `h2k.producer.batch.debug.on.failure` — печатать подробности ошибок авто‑сброса в DEBUG (по умолчанию `false`).

### Kafka Producer (pass‑through)  
Любые ключи с префиксом `h2k.producer.*` прокидываются в `KafkaProducer` как нативные. Наиболее важные:
- `h2k.producer.acks` — `0`/`1`/`all`
- `h2k.producer.enable.idempotence` — `true`/`false`
- `h2k.producer.max.in.flight` — `1` для строгого порядка; `2..5` быстрее
- `h2k.producer.linger.ms`, `h2k.producer.batch.size`
- `h2k.producer.compression.type` — **рекомендуем `lz4`**
- `h2k.producer.retries`, `delivery.timeout.ms`, `request.timeout.ms`, `max.block.ms`, `buffer.memory`, `client.id`, ...

> **Важно про `client.id`:** не задавайте фиксированное значение. `KafkaReplicationEndpoint` сам сформирует **уникальный** `client.id` на каждой ноде (по `hostname`, а при проблемах с DNS — по `UUID`). Это исключает конфликты и делает метрики понятнее. Если всё‑таки задаёте вручную — обеспечьте уникальность на ноду.

> Полные рабочие примеры см. ниже (раздел «Профили peer»).

### Шпаргалка по ключам (сводная таблица)

| Ключ | Дефолт | Где применяется | Назначение / примечание |
|---|---|---|---|
| `h2k.kafka.bootstrap.servers` | — (обязательно) | KafkaReplicationEndpoint → KafkaProducer | Брокеры Kafka, список `host:port` через запятую |
| `h2k.topic.pattern` | `${table}` | KafkaReplicationEndpoint | Шаблон имени топика; поддерживает `${table}`, `${namespace}`, `${qualifier}` |
| `h2k.cf` | `0` | PayloadBuilder | Целевой ColumnFamily (индекс) |
| `h2k.salted` | `false` | PhoenixRowKeyDecoder | Признак SALT в rowkey |
| `h2k.salt.bytes` | `1` | PhoenixRowKeyDecoder | Размер SALT (байт), если `salted=true` |
| `h2k.decode.mode` | `simple` | KafkaReplicationEndpoint | Режим декодера: `simple` или `json-phoenix` |
| `h2k.schema.path` | — (обязательно при `json-phoenix`) | JsonSchemaRegistry | Путь к `schema.json` на RS |
| `h2k.json.serialize.nulls` | `false` | KafkaReplicationEndpoint (Gson) | Сериализовать ли `null` в JSON |
| `h2k.include.meta` | `false` | PayloadBuilder | Добавлять служебные поля `_table/_cf/_cells_total` и пр. |
| `h2k.include.meta.wal` | `false` | PayloadBuilder | Добавлять `_wal_seq/_wal_write_time` |
| `h2k.include.rowkey` | `false` | PayloadBuilder | Добавлять rowkey в JSON |
| `h2k.rowkey.base64` | `true` | PayloadBuilder | Формат rowkey при `include.rowkey=true` (`true`=Base64, `false`=hex) |
| `h2k.filter.by.wal.ts` | `false` | KafkaReplicationEndpoint | Включить фильтрацию по минимальному WAL timestamp |
| `h2k.wal.min.ts` | `-1` | KafkaReplicationEndpoint | Минимальный `timestamp` (epoch ms) для фильтра |
| `h2k.producer.await.every` | `500` | BatchSender | Дозированное ожидание ack каждые N send |
| `h2k.producer.await.timeout.ms` | `180000` | BatchSender | Общий таймаут ожидания ack (мс) |
| `h2k.producer.batch.counters.enabled` | `false` | BatchSender | Включить лёгкие счётчики (диагностика) |
| `h2k.producer.batch.debug.on.failure` | `false` | BatchSender | Подробности авто‑сброса в DEBUG |
| `h2k.ensure.topics` | `false` | TopicEnsurer | Автопроверка/создание топиков при старте |
| `h2k.topic.partitions` | — | TopicEnsurer | Число партиций (используется при создании темы) |
| `h2k.topic.replication` | — | TopicEnsurer | Фактор репликации (при создании темы) |
| `h2k.admin.timeout.ms` | `30000` | TopicEnsurer/AdminClient | Таймаут операций Kafka Admin |
| `h2k.ensure.unknown.backoff.ms` | `5000` | TopicEnsurer | Короткий backoff на «неуверенные» ошибки |
| `h2k.topic.config.*` | — | TopicEnsurer | Pass‑through свойств создаваемого топика (`retention.ms`, `cleanup.policy`, и т.д.) |
| `h2k.producer.*` | Kafka default | KafkaProducer | Любые нативные свойства продьюсера (`acks`, `enable.idempotence`, `linger.ms`, `batch.size`, `compression.type`, `delivery.timeout.ms`, `retries`, `request.timeout.ms`, `buffer.memory`, **`client.id`** и т.д.) |

### Логи — JVM‑флаги (не из `H2kConfig`)

| Флаг JVM | Дефолт | Назначение |
|---|---|---|
| `-Dh2k.log.dir` | `logs` | Каталог для файлов логов |
| `-Dh2k.log.profile` | `default` | Профиль имени файла: `default`\|`fast`\|`reliable` (см. RollingFileAppender) |
| `-Dh2k.log.maxFileSize` | `64MB` | Размер файла до ротации |
| `-Dh2k.log.maxBackupIndex` | `10` | Количество файлов‑бэкапов |

---

## Профили peer и готовые команды

В примерах ниже подставьте ваши ZK/пути. Для тестового стенда:  
**ZK:** `10.254.3.111,10.254.3.112,10.254.3.113:2181:/hbase`  
**Kafka:** `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`

### 1) FAST — максимальная скорость
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

# скорость > надёжность
java_conf.put("h2k.producer.acks", "1")
java_conf.put("h2k.producer.enable.idempotence", "false")
java_conf.put("h2k.producer.max.in.flight", "5")
java_conf.put("h2k.producer.linger.ms", "20")
java_conf.put("h2k.producer.batch.size", "131072")   # 128 KiB
java_conf.put("h2k.producer.compression.type", "lz4")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer_fast", repconf, java.util.HashMap.new)
```

### 2) BALANCED — компромисс
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

# компромисс скорость/надёжность
java_conf.put("h2k.producer.acks", "1")
java_conf.put("h2k.producer.enable.idempotence", "true")
java_conf.put("h2k.producer.max.in.flight", "2")
java_conf.put("h2k.producer.linger.ms", "50")
java_conf.put("h2k.producer.batch.size", "65536")    # 64 KiB
java_conf.put("h2k.producer.compression.type", "lz4")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer_balanced", repconf, java.util.HashMap.new)
```

### 3) RELIABLE — строгие гарантии
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

# приоритет — порядок и не‑потеря
java_conf.put("h2k.producer.acks", "all")
java_conf.put("h2k.producer.enable.idempotence", "true")
java_conf.put("h2k.producer.max.in.flight", "1")
java_conf.put("h2k.producer.linger.ms", "50")
java_conf.put("h2k.producer.batch.size", "65536")
java_conf.put("h2k.producer.compression.type", "lz4")
java_conf.put("h2k.producer.delivery.timeout.ms", "180000")
java_conf.put("h2k.producer.retries", "2147483647")

rep_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(@hbase.configuration)
rep_admin.addPeer("kafka_peer_reliable", repconf, java.util.HashMap.new)
```

---

## Формат сообщения (JSONEachRow)
На каждую строку (rowkey) — одна JSON‑строка:
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
- `c`, `t`, `opd_ms` — части PK из Phoenix rowkey (`VARCHAR`, `UNSIGNED_TINYINT`, `TIMESTAMP` → миллисекунды).
- `event_version` — максимальный `timestamp` среди ячеек нашего CF.
- `delete=true` — если пришёл delete‑маркер по CF.
- Остальные `qualifier → value` получаются через выбранный `Decoder`.

---

## Схема (`schema.json`)
- Пример:
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
- На имена таблиц публикуются алиасы: `orig/UPPER/lower` + «короткое» имя после `:` (и его `UPPER/lower`).
- На имена колонок — также `orig/UPPER/lower`. Типы канонизируются в `UPPER` (`Locale.ROOT`).
- Схема иммутабельна; `refresh()` переинициализирует снимок (или перезапуск RS).

---

## Зависимости на кластере (проверка)

Endpoint собирается с `scope=provided` для базовых библиотек (они уже есть на RS).  
Минимальный набор JAR на **каждом RS**:
- HBase 1.4.13 (`hbase-server-1.4.13.jar` и пр.) — **есть по умолчанию**
- Hadoop Common 2.7.4 — **есть по умолчанию**
- Phoenix Core 4.14/4.15 — если используете `json-phoenix` (например, symlink `phoenix-HBase-server.jar`)
- SLF4J 1.7.x — **есть**
- **Kafka Clients 2.3.1** — может отсутствовать → добавить
- **lz4-java 1.6.0+** — может отсутствовать → добавить
- (Опционально) `snappy-java` — уже есть

Быстрая проверка:
```bash
# что уже подхватывает RS
hbase classpath | tr ':' '\n' | egrep -i 'kafka-clients|lz4|snappy|gson|phoenix-core'

# если нет kafka-clients/lz4 — скопируйте из каталога Kafka и перезапустите RS:
# пути примера:
cp /opt/kafka-default-current/libs/kafka-clients-2.3.1.jar /opt/hbase-default-current/lib/
cp /opt/kafka-default-current/libs/lz4-java-1.6.0.jar    /opt/hbase-default-current/lib/
```

---

## Логирование

Мы используем Log4j с консольным выводом и ротацией файлов (RollingFileAppender). Все настройки уже добавлены в `src/main/resources/log4j.properties`.

### Что настроено по умолчанию
- Кодировка **UTF‑8** (русские сообщения без кракозябр).
- Паттерн: `%d{ISO8601} %-5p [%t] %c - %m%n` (без дорогих `%M/%L`).
- Ротация файлов:
  - путь: `${h2k.log.dir}/h2k-endpoint-${h2k.log.profile}.log`;
  - размер одного файла: `${h2k.log.maxFileSize}` (дефолт: `64MB`);
  - количество бэкапов: `${h2k.log.maxBackupIndex}` (дефолт: `10`).

### Как переключать профили логов
Эти параметры **не относятся к `H2kConfig`** и управляются **JVM‑флагами**.

**Через HBASE_OPTS** (в `hbase-env.sh` или перед запуском сервиса):
```bash
export HBASE_OPTS="$HBASE_OPTS -Dh2k.log.dir=/var/log/h2k -Dh2k.log.profile=fast -Dh2k.log.maxFileSize=128MB -Dh2k.log.maxBackupIndex=20"
```
Доступные профили: `default | fast | reliable` (или свой).

**Через systemd override (рекомендуется):**
```bash
sudo mkdir -p /etc/systemd/system/hbase-regionserver.service.d
sudo tee /etc/systemd/system/hbase-regionserver.service.d/10-h2k-logs.conf >/dev/null <<'EOF'
[Service]
Environment="HBASE_OPTS=${HBASE_OPTS} -Dh2k.log.dir=/var/log/h2k -Dh2k.log.profile=reliable -Dh2k.log.maxFileSize=128MB -Dh2k.log.maxBackupIndex=20"
EOF
sudo systemctl daemon-reload
sudo systemctl restart hbase-regionserver
```
Аналогично можно сделать override для Master: `hbase-master.service.d/10-h2k-logs.conf`.

**Уровни сторонних библиотек**: по умолчанию заглушены до `WARN`, наш пакет — `INFO`. Точечный DEBUG можно включать в `log4j.properties` для отдельных классов.

---

## Диагностика и отладка

**Проверка репликации (HBase shell):**
```bash
status 'replication'
list_peers
```

**Полезные операции:**
```bash
# включить/выключить
enable_peer 'kafka_peer_fast'
disable_peer 'kafka_peer_fast'

# обновить конфиг (например, поменяли acks или bootstrap)
update_peer_config 'kafka_peer_fast', repconf
```

**Логи RS (русские сообщения):**
- Инициализация:
  ```
  INFO  KafkaReplicationEndpoint initialized: topicPattern=${table}, cf=0, salted=false, saltBytes=1
  INFO  Decode mode=json-phoenix, schema=/etc/h2k/schema.json
  ```
- Предупреждения схемы:
  ```
  WARN  Схема пуста или имеет неожиданный формат — реестр останется пустым
  WARN  Обнаружен дубликат таблицы '...' (ключ '...') в файле схемы '...' — существующее определение будет перезаписано
  ```
- Ошибки Kafka:
  ```
  ERROR replicate() timed out while waiting on Kafka futures
  ```

**JMX / метрики:**
- `Hadoop:service=HBase,name=RegionServer,sub=Replication` — задержки, очереди.
- `kafka.producer:type=producer-metrics,client-id=*` и `...producer-topic-metrics...`.

**Быстрая проверка Kafka:**
```bash
kafka-console-consumer.sh \
  --bootstrap-server 10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092 \
  --topic <ваш_топик> --from-beginning --max-messages 5
```

---

## «Что тюнить, если…»

- **Пики задержек**  
  Снизьте `h2k.producer.linger.ms`; проверьте сеть и GC; при `acks=all` — здоровье ISR/диски брокеров.
- **Ошибки брокера** (`TimeoutException`, `NotEnoughReplicas`)  
  Проверьте кворум ISR, увеличьте `delivery.timeout.ms`, уменьшите `max.in.flight` и `batch.size`.
- **Переполнения буфера** (`BufferExhaustedException`)  
  Увеличьте `buffer.memory`, уменьшайте `linger.ms`/`batch.size`, включите/усильте компрессию (`lz4`).

---

## Архитектура и производительность (кратко)
- **KafkaReplicationEndpoint** — группировка `WALEdit` по rowkey без `String`; одна копия rowkey на запись; дозированное ожидание ack.
- **PhoenixRowKeyDecoder** — быстрый парсер PK (`c,t,opd_ms`), поддержка SALT, только ASC‑колонки.
- **JsonSchemaRegistry** — иммутабельная схема + кэш `TableName→columns`.
- **Decoder/SimpleDecoder/ValueCodecPhoenix** — интерфейсы без лишних аллокаций, режим `json-phoenix` использует схему.

**JVM рекомендации:** `-XX:+UseG1GC -XX:MaxGCPauseMillis=50`, `-XX:+AlwaysPreTouch`.

---

## FAQ

**Зачем `scope=provided` в POM?**  
Чтобы не таскать в наш JAR HBase/Hadoop/Phoenix/Kafka‑клиенты, которые уже стоят на RS. Меньше конфликтов и размер артефакта.  
Если какого‑то JAR нет — добавьте его в `.../lib` и перезапустите RS.

**Snappy или LZ4?**  
Для нашего профиля нагрузки чаще выигрывает **LZ4** (меньше CPU/latency при сопоставимой компрессии). Поэтому примеры используют `lz4`.

**Нужно ли класть `META‑INF/MANIFEST.MF` в git?**  
Нет. Он генерируется Maven’ом. В `.gitignore` оставляем `target/` и любые локальные артефакты сборки.

---

## Быстрый чек‑лист запуска
1. JAR лежит в `/opt/hbase-default-current/lib/`.
2. На RS есть `kafka-clients-2.3.1.jar` и `lz4-java-1.6.0+.jar` (проверили через `hbase classpath`).
3. Создан peer (`fast/balanced/reliable`) с корректным `bootstrap.servers`.
4. Если `json-phoenix` — `schema.json` доступен и путь указан.
5. В логах RS нет ошибок и события появляются в Kafka.

---

## Ограничения
- Phoenix PK: только **ASC**‑колонки.
- Подключение к Kafka — по умолчанию **PLAINTEXT**.

---

## Лицензия
См. `LICENSE` (если присутствует в репозитории).