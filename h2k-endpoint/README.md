## HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (JSONEachRow)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Endpoint‑класс:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x, публикующий **одну JSON‑строку на строку HBase** (формат JSONEachRow). Минимум аллокаций, стабильный порядок ключей, дружелюбные логи на русском.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Поддерживаемые версии](#поддерживаемые-версии)
- [Установка](#установка)
- [Минимальная конфигурация](#минимальная-конфигурация)
- [Профили peer](#профили-peer)
- [Полная документация](#полная-документация)

---

## Быстрый старт

1) **Соберите и разложите JAR** на все RegionServer:
```bash
mvn -q -DskipTests clean package
cp target/h2k-endpoint-*.jar /opt/hbase-default-current/lib/
```

2) **(Опционально) Подготовьте Phoenix‑схему** для режима `json-phoenix`:  
`/opt/hbase-default-current/conf/schema.json`.

3) **Включите репликацию CF** в нужных таблицах и глобально:
```xml
<!-- hbase-site.xml (на RS) -->
<property><name>hbase.replication</name><value>true</value></property>
```
```HBase shell
# пример: включить CF 'd'
disable 'TBL_JTI_TRACE_CIS_HISTORY'
alter  'TBL_JTI_TRACE_CIS_HISTORY', { NAME => 'd', REPLICATION_SCOPE => 1 }
enable 'TBL_JTI_TRACE_CIS_HISTORY'
```

4) **Создайте peer** готовым скриптом (рекомендуется BALANCED):
```bash
bin/hbase shell conf/add_peer_shell_balanced.txt
```

5) **Проверьте доставку**: сообщения появляются в топике `${table}`.

---

## Поддерживаемые версии

- **Java:** 8 (target 1.8)
- **HBase:** 1.4.13 (совместимо с 1.4.x)
- **Kafka (клиенты):** 2.3.1
- **Phoenix:** 4.14/4.15 (только для `json-phoenix`)

> RegionServer и Endpoint должны работать на **Java 8**.

---

## Установка

1. Скопируйте JAR в `/opt/hbase-default-current/lib/`.  
2. Убедитесь, что на RS есть зависимости (scope=provided):
   - `kafka-clients-2.3.1.jar`
   - `lz4-java-1.6.0+.jar` (для FAST/BALANCED)
   - `snappy-java-1.1.x+.jar` (для RELIABLE, если `compression.type=snappy`)
3. Перезапустите RegionServer.

Быстрая проверка:
```bash
hbase classpath | tr ':' '\n' | egrep -i 'kafka-clients|lz4|snappy'
```

---

## Минимальная конфигурация

**Откуда читаются ключи:**  
- системный `hbase-site.xml` (дефолты),  
- *и/или* карта `CONFIG` у peer (имеет приоритет).

**Минимально для запуска** (пример для `TBL_JTI_TRACE_CIS_HISTORY`, CF `d`):
```properties
h2k.kafka.bootstrap.servers=10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092
h2k.cf.list=d
# Декодирование:
h2k.decode.mode=json-phoenix
h2k.schema.path=/opt/hbase-default-current/conf/schema.json
# Подсказки/соль (по необходимости):
h2k.capacity.hints=TBL_JTI_TRACE_CIS_HISTORY=32
h2k.salt.map=TBL_JTI_TRACE_CIS_HISTORY=1
# Топик:
h2k.ensure.topics=true
h2k.topic.pattern=${table}
```

**Ключевые опции (коротко):**

| Ключ | Назначение | Примечание |
|---|---|---|
| `h2k.kafka.bootstrap.servers` | Список брокеров Kafka | `host:port[,host2:port2]` |
| `h2k.cf.list` | Список CF для экспорта | CSV |
| `h2k.decode.mode` | `simple` \| `json-phoenix` | Для Phoenix нужен `schema.json` |
| `h2k.schema.path` | Путь к `schema.json` | Только для `json-phoenix` |
| `h2k.topic.pattern` | Шаблон имени топика | `${table}` по умолчанию |
| `h2k.ensure.topics` | Автосоздание тем | true/false |
| `h2k.payload.include.meta` | Добавлять служебные поля | +`event_version`,`delete` и т.д. |
| `h2k.payload.include.meta.wal` | Добавлять `_wal_seq`,`_wal_write_time` | требует включить meta |
| `h2k.payload.include.rowkey` | Включать `_rowkey` | `BASE64`/`HEX` управляется `h2k.rowkey.encoding` |

> Полная справка по ключам и значениям — см. **docs/config.md**.

---

## Профили peer

Готовые скрипты (каталог `conf/`):

- **BALANCED** — `conf/add_peer_shell_balanced.txt` (рекомендуется для прод)
- **RELIABLE** — `conf/add_peer_shell_reliable.txt` (строгие гарантии/порядок)
- **FAST** — `conf/add_peer_shell_fast.txt` (максимальная скорость)

Краткие отличия:

| Профиль | acks | idempotence | max.in.flight | compression | batch/linger |
|---|---|---|---|---|---|
| FAST | `1` | `false` | `5` | `lz4` | `524288 / 100ms` |
| BALANCED | `all` | `true` | `5` | `lz4` | `524288 / 100ms` |
| RELIABLE | `all` | `true` | `1` | `snappy` | `65536 / 50ms` |

> Подробная матрица и команды администрирования — см. **docs/peer-profiles.md** и **docs/hbase.md**.

---

## Полная документация

- [Конфигурация (все ключи)](docs/config.md)
- [Phoenix и `schema.json`](docs/phoenix.md)
- [Подсказки ёмкости и метаданные](docs/capacity.md)
- [HBase shell / ZooKeeper / операции](docs/hbase.md), а также [операции эксплуатации](docs/operations.md)
- [ClickHouse ingest (JSONEachRow)](docs/clickhouse.md)
- [Диагностика и типовые ошибки](docs/troubleshooting.md)
- [Профили peer (полная матрица)](docs/peer-profiles.md)
- [Roadmap (Avro/Schema Registry)](docs/roadmap-avro.md)
