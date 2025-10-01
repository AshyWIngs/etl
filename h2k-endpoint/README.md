## HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (AVRO — целевой формат; JSONEachRow — вспомогательный)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Endpoint‑класс:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x. Основной режим — **AVRO** (Generic / Confluent Schema Registry), что соответствует целевой архитектуре. Формат **JSONEachRow** также поддерживается, но используется в основном для отладки и интеграций без Avro. Минимум аллокаций, стабильный порядок ключей, дружелюбные логи на русском.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Поддерживаемые версии](#поддерживаемые-версии)
- [Установка](#установка)
- [Минимальная конфигурация](#минимальная-конфигурация)
- [Профили peer](#профили-peer)
- [Полная документация](#полная-документация)
- [Поддержка форматов сообщений](#поддержка-форматов-сообщений)
- [Безопасность и ограничения](#безопасность-и-ограничения)
- [Раскатка и мониторинг](#раскатка-и-мониторинг)
- [FAQ](#faq)
- [Структура пакетов](#структура-пакетов)

---

## Быстрый старт

1) **Соберите и разложите JAR** на все RegionServer (JAR уже содержит Avro/Jackson/Confluent 5.3.8):
```bash
mvn -q -DskipTests clean package
cp target/h2k-endpoint-*.jar /opt/hbase-default-current/lib/
```

2) **Подготовьте Avro‑схемы** с атрибутами `h2k.phoenixType` и массивом `h2k.pk` в каталоге `conf/avro` (см. docs/avro.md).  
   На период миграции можно держать `schema.json` (фолбэк для `phoenix-avro`).

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

5) **Проверьте доставку**: сообщения появляются в топике `${table}` (по умолчанию в формате AVRO; при необходимости можно выбрать JSONEachRow).

---

## Поддерживаемые версии

- **Java:** 8 (target 1.8)
- **HBase:** 1.4.13 (совместимо с 1.4.x)
- **Kafka (клиенты):** 2.3.1
- **Phoenix:** 4.14/4.15 (совместимо; режим `json-phoenix` поддерживается как legacy)

> RegionServer и Endpoint должны работать на **Java 8**.

---

## Установка

1. Скопируйте JAR в `/opt/hbase-default-current/lib/`.  
2. Убедитесь, что на RS есть базовые клиентские библиотеки из кластера:
   - `kafka-clients-2.3.1.jar`
   - `lz4-java-1.6.0+.jar` (для FAST/BALANCED)
   - `snappy-java-1.1.x+.jar` (для RELIABLE, если `compression.type=snappy`)
   > Avro/Jackson и Confluent Schema Registry 5.3.8 шейдятся внутрь `h2k-endpoint-*.jar`, поэтому дополнительные Confluent JAR не нужны.
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
h2k.payload.format=avro-binary
# Декодирование:
h2k.decode.mode=phoenix-avro
# Фолбэк на schema.json (опционально на время миграции)
h2k.schema.path=/opt/hbase-default-current/conf/schema.json
# Топик:
h2k.ensure.topics=true
h2k.topic.pattern=${table}
```

**Ключевые опции (коротко):**

| Ключ | Назначение | Примечание |
|---|---|---|
| `h2k.payload.format` | Формат сериализации payload | `avro-binary` (рекомендуется) \| `json-each-row` |
| `h2k.kafka.bootstrap.servers` | Список брокеров Kafka | `host:port[,host2:port2]` |
| `h2k.cf.list` | Список CF для экспорта | CSV; пробелы обрезаются, регистр сохраняется |
| `h2k.decode.mode` | `simple` \| `phoenix-avro` \| `json-phoenix` (legacy) | `phoenix-avro` считывает типы/PK из `.avsc`, `schema.json` нужен только для фолбэка |
| `h2k.schema.path` | Путь к `schema.json` | Необязательный фолбэк для `phoenix-avro`, обязателен для `json-phoenix` |
| `h2k.salt.map` | Карта соли rowkey | Опциональный фолбэк; основной источник — `.avsc` (`h2k.saltBytes`) |
| `h2k.capacity.hints` | Подсказки ёмкости JSON | Опциональный фолбэк; основной источник — `.avsc` (`h2k.capacityHint`) |
| `h2k.topic.pattern` | Шаблон имени топика | `${table}` по умолчанию |
| `h2k.ensure.topics` | Автосоздание тем | true/false |
| `h2k.payload.include.meta` | Добавлять служебные поля | +`_event_ts`,`_delete` и т.д. |
| `h2k.payload.include.meta.wal` | Добавлять `_wal_seq`,`_wal_write_time` | требует включить meta |
| `h2k.payload.include.rowkey` | Включать `_rowkey` | `BASE64`/`HEX` управляется `h2k.rowkey.encoding` |


> Endpoint фильтрует WAL только по указанным CF и не меняет регистр имён. Если ключ `h2k.cf.list`
> не задан — реплицируются все CF. При явном списке, но ошибке в регистре, записи соответствующего
> семейства будут отброшены.

> Режим `phoenix-avro` ожидает, что локальные `.avsc` содержат атрибуты `h2k.phoenixType` для колонок и массив `h2k.pk`.
> При наличии `h2k.schema.path` эти данные используются как фолбэк на период миграции.

> Полная справка по ключам и значениям — см. **docs/config.md**.

### Матрица продьюсерских профилей (значения синхронизированы с `conf/`)

| Ключ | Единицы | Дефолт (Endpoint) | FAST (`conf/add_peer_shell_fast.txt`) | BALANCED (`conf/add_peer_shell_balanced.txt`) | RELIABLE (`conf/add_peer_shell_reliable.txt`) |
|---|---|---|---|---|---|
| `h2k.producer.enable.idempotence` | boolean | `true` | `false` | `true` | `true` |
| `h2k.producer.acks` | ack mode | `all` | `1` | `all` | `all` |
| `h2k.producer.max.in.flight` | запросов | `1` | `5` | `5` | `1` |
| `h2k.producer.linger.ms` | миллисекунды | `50` | `100` | `100` | `50` |
| `h2k.producer.batch.size` | байты | `65536` | `524288` | `524288` | `65536` |
| `h2k.producer.compression.type` | алгоритм | `lz4` | `lz4` | `lz4` | `snappy` |
| `h2k.producer.delivery.timeout.ms` | миллисекунды | `180000` | `90000` | `300000` | `300000` |

> Таблица отражает реальные значения из скриптов `conf/add_peer_shell_*.txt`; изменения в профилях необходимо синхронизировать с документацией.

---

## Профили peer

Готовые скрипты (каталог `conf/`):

- **BALANCED** — `conf/add_peer_shell_balanced.txt` (рекомендуется для прод)
- **RELIABLE** — `conf/add_peer_shell_reliable.txt` (строгие гарантии/порядок)
- **FAST** — `conf/add_peer_shell_fast.txt` (максимальная скорость)

Краткая матрица (полные значения и примеры — в [docs/peer-profiles.md](docs/peer-profiles.md)):

| Профиль | Назначение | `h2k.payload.format` | `acks` | `enable.idempotence` | `max.in.flight` | `linger.ms` | `batch.size` | `compression` |
|---|---|---|---|---|---|---|---|---|
| FAST | скорость, допускаем дубль | `json-each-row` (Avro опционально) | `1` | `false` | `5` | `100` | `524288` | `lz4` |
| BALANCED | прод Avro по умолчанию | `avro-binary` + `avro.mode=generic` | `all` | `true` | `5` | `100` | `524288` | `lz4` |
| RELIABLE | строгий порядок | `json-each-row` (Avro опционально) | `all` | `true` | `1` | `50` | `65536` | `snappy` |

> Дополнительные параметры и подсказки по тюнингу см. в **docs/peer-profiles.md** и **docs/hbase.md**.

---

## Полная документация

- [Конфигурация (все ключи)](docs/config.md)
- [Phoenix и `schema.json`](docs/phoenix.md)
- [Подсказки ёмкости и метаданные](docs/capacity.md)
- [HBase shell / ZooKeeper / операции](docs/hbase.md), а также [операции эксплуатации](docs/runbook/operations.md)
- [ClickHouse ingest (JSONEachRow)](docs/clickhouse.md)
- [Avro (локальные схемы / Confluent)](docs/avro.md)
- [Профильные скрипты и матрица настроек](docs/peer-profiles.md)

## Поддержка форматов сообщений

Endpoint умеет формировать payload в нескольких форматах (управляется ключом `h2k.payload.format`):

- `avro-binary` — **рекомендуемый** формат для продакшена.
  - `h2k.avro.mode=generic` — локальные `.avsc` из каталога `h2k.avro.schema.dir`.
  - `h2k.avro.mode=confluent` — работа через Confluent Schema Registry 5.3.x (payload к SR экранируется: кавычки, `\n`, control-символы кодируются по JSON). Подробная конфигурация — см. `docs/avro.md`.
- `json-each-row` — простой текстовый формат, удобный для отладки и ClickHouse ingest.
- `avro-json` — Avro в JSON-представлении, также в основном для отладки.

| Ключ | Значение по умолчанию | Комментарий |
|---|---|---|
| `h2k.avro.mode` | `confluent` | `generic` использует локальные `.avsc` |
| `h2k.avro.schema.dir` | `conf/avro` | Каталог локальных схем и fallback‑кэш SR |
| `h2k.avro.sr.urls` | — | CSV вида `http://sr1:8081,http://sr2:8081` |
| `h2k.avro.props.subject.strategy` | `table` | `table` → `namespace:table`, `qualifier` — прежнее поведение |
| `h2k.avro.props.subject.prefix` | пусто | Используйте для разделения окружений |
| `h2k.avro.props.subject.suffix` | пусто | Часто ставят `-value` для совместимости с Kafka Connect |
| `h2k.avro.props.client.cache.capacity` | `1000` | Размер identity-map `CachedSchemaRegistryClient` |
| `h2k.avro.props.basic.username`/`password` | — | Базовая авторизация, значения маскируются в логах |

При старте endpoint выводит строку уровня INFO вида `Payload: payload.format=..., serializer.class=..., avro.mode=...` — по ней видно, активен ли AVRO, какой режим и откуда берутся схемы. Сообщение появляется независимо от включённого DEBUG.

## Безопасность и ограничения

- **SASL/SSL отсутствуют.** Endpoint работает в выделенной сети Kafka/HBase; шифрование и аутентификация обеспечиваются внешней инфраструктурой (VPN, ACL на брокерах, firewall).
- **Только задокументированные ключи.** Любые новые параметры добавляются через конфигурацию `h2k.*` и описываются в `docs/`, магии нет.
- **Секреты не храним.** Пароли Schema Registry передаём через `CONFIG` peer или переменные окружения RS; в логах значения маскируются.
- **Совместимость с Java 8.** Код не использует API 9+, коллекции предразмеряются по `h2k.capacity.hints` для экономии GC.
- **Локализация.** Логи, исключения и JavaDoc — на русском языке; структура комментариев отражает бизнес-логику.

## Раскатка и мониторинг

1. **Staged rollout.** Раскатайте JAR на один RegionServer, включите peer только для части таблиц и убедитесь, что Schema Registry зарегистрировал схемы без конфликтов.
2. **Метрики GC/throughput.** Снимите `HRegionServer.GcTimeMillis`, `ReplicationSource.avgReplicationDelay`, а также нагрузку на Kafka продьюсер (`records-sec`, `request-latency`).
3. **Наблюдаемость Kafka.** Проверяйте `UnderReplicatedPartitions`, `RecordErrorRate`, ошибки `org.apache.kafka.clients` в логах RS.
4. **Метрики endpoint.** `TopicManager.getMetrics()` возвращает счётчики ensure и свежие показатели `wal.*`, `schema.registry.*`; INFO-лог `WAL throughput: ...` появляется примерно каждые 5 секунд и показывает фактическую скорость строк/сек.
5. **Расширение.** После 1–2 часов без аномалий включайте остальные RS и таблицы; держите предыдущую версию JAR в каталоге `lib/backup` до полного завершения миграции.
6. **Откат.** При необходимости отключите peer (`disable_peer`), удалите новый JAR, верните предыдущий и перезапустите RS.

## FAQ

**Нужно ли класть Confluent JAR на RegionServer?**  
Нет. Версия 0.0.12 шейдит внутрь `io.confluent:kafka-avro-serializer` и `kafka-schema-registry-client` 5.3.8 вместе с зависимостями. Достаточно базовых библиотек кластера (`kafka-clients`, `lz4`, `snappy`).

**Что делать при ошибке 409 от Schema Registry?**  
По умолчанию мы используем `subject.strategy=table`, т.е. `namespace:table` (для `default` остаётся просто имя таблицы). Если ранее использовались сабджекты вида `qualifier`, проверьте `h2k.avro.subject.*`: можно вернуть старую стратегию, либо добавить префикс/суффикс. При конфликте удалите неверную версию в SR и перезапустите peer.

**Поддерживается ли SASL/SSL?**  
Нет. Endpoint рассчитан на доверенную сеть без TLS. Для защиты используйте внешний TLS proxy или переезд на защищённый кластер Kafka; настройки безопасности внутрь endpoint не добавляются.

**Как обновить локальные Avro-схемы?**  
Положите новую `.avsc` в `conf/avro/<table>.avsc`, убедитесь, что она обратимо совместима, и перезапустите peer. Endpoint перечитает схему при первом попадании таблицы в cache.

---

## Структура пакетов

- `kz.qazmarka.h2k.kafka.ensure` — фасады `TopicEnsurer`/`TopicEnsureService`; вспомогательные классы распределены по подпакетам `admin`, `planner`, `state`, `metrics`, `config`, `util`.
- `kz.qazmarka.h2k.payload.serializer` — реализация сериализаторов и внутреннего резолвера (`serializer.internal.SerializerResolver`).
- `kz.qazmarka.h2k.schema.registry.json` — JSONEachRow (Schema Registry JSON / внешние JSON‑схемы).
- `kz.qazmarka.h2k.schema.registry.avro.local` — локальные `.avsc` для `payload.format=avro-*` в режиме `generic`.
- `kz.qazmarka.h2k.schema.registry.avro.phoenix` — Phoenix‑метаданные из Avro (соль, PK) для Avro режимов.
- `docs/runbook` — эксплуатационный runbook и диагностика; ссылки синхронизированы с конфигами в `conf/`.
