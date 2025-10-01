# Avro: локальные схемы и Confluent Schema Registry

Поддержка Avro включается ключом `h2k.payload.format` и работает в двух режимах:

- `h2k.avro.mode=generic` — читаем локальные `.avsc` из каталога.
- `h2k.avro.mode=confluent` — регистрируем/используем схемы в Confluent Schema Registry 5.3.8 (Kafka 2.3.1, Java 8).

## Локальные `.avsc` (`generic`)

1. **Структура каталога** — по умолчанию `conf/avro`. Для каждой таблицы создаём файл
   `conf/avro/<table>.avsc`. Имя таблицы берётся без namespace (если namespace `DEFAULT`) либо
   `ns:table` (нижний регистр). Путь можно переопределить ключом `h2k.avro.schema.dir`.

```
conf/avro/
  t_receipt.avsc
  default:t_receipt_items.avsc
```

2. **Конфигурация**: минимальный набор ключей.

```properties
h2k.payload.format=avro-binary# или avro-json
h2k.avro.mode=generic
h2k.avro.schema.dir=/opt/hbase/conf/avro
   ```

3. **Проверка** — используйте `bin/hbase classpath` для проверки наличия Avro-зависимостей и
   команду `mvn -q -DskipITs -Dtest=GenericAvroPayloadSerializerTest test` для локальной валидации.

4. **Phoenix-метаданные** — добавьте к каждому полю Avro атрибут `"h2k.phoenixType"` (строгое имя Phoenix-типа),
   на корневом уровне опишите массив `"h2k.pk"`, а также дополнительные свойства `"h2k.saltBytes"`
   (0..8) и `"h2k.capacityHint"` (ожидаемое число полей в JSON). Все значения считываются
   `AvroPhoenixSchemaRegistry`; `schema.json` остаётся резервным источником на период миграции.

## Confluent Schema Registry (`confluent`)

1. **Версии** — SR 5.3.8 (совместимый с Kafka clients 2.3.1, Java 8). Wire-format:
   `MAGIC_BYTE (0x0)` + `int32 schemaId` + `avroBinary`.

2. **Конфигурация** (минимальный пример):

```properties
# Формат и режим
h2k.payload.format=avro-binary
h2k.avro.mode=confluent
# Локальные .avsc всё равно полезны для валидации при старте
h2k.avro.schema.dir=/opt/hbase/conf/avro
# Список SR-узлов (кластер 5.3.8)
h2k.avro.sr.urls=http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081
# Как формируем subject для value-схемы:
# Таблица у нас TBL_JTI_TRACE_CIS_HISTORY, ожидаемый subject — TBL_JTI_TRACE_CIS_HISTORY-value
h2k.avro.subject.strategy=table-upper
# Возможные значения:
# qualifier— использовать namespace
# table — имя таблицы
# table-lower — имя таблицы в нижнем регистре
# table-upper — имя таблицы в верхнем регистре
h2k.avro.subject.suffix=-value
```

   > В режиме `confluent` поддерживается только `h2k.payload.format=avro-binary`.
   > Итог: в Kafka идёт payload вида 0x00 + int32(schemaId) + avroBinary, где schemaId берётся из SR при первой регистрации. Повторные публикации используют кэшированный id.

3. **Subject strategy** — по умолчанию используется `table`, что формирует `namespace:table` (для namespace `default` остаётся просто qualifier). Для совместимости со старыми раскладками доступны варианты `qualifier`, `table-lower`, `table-upper`, а также префикс/суффикс.

4. **Кэш клиента** — параметр `h2k.avro.props.client.cache.capacity` управляет размером identity-map `CachedSchemaRegistryClient` (дефолт 1000). Увеличивайте его, если в пуле много разных subject'ов и активна быстрая смена схем.

5. **Расширенная авторизация** — пока поддерживается только Basic. Для TLS/OAuth добавьте ключи
   в секцию `h2k.avro.sr.auth.*` и реализуйте обработку при необходимости.

6. **Экранирование JSON** — перед отправкой в Schema Registry локальная схема проходит безопасное
   экранирование: кавычки, управляющие символы (`\n`, `\r`, `\t`, `\u0000`…`\u001F`, `\u2028/\u2029`)
   кодируются по правилам JSON. Это устраняет проблемы с doc/description, где встречаются переносы и
   управляющие символы, и гарантирует валидный payload для SR 5.3.8.

7. **Диагностика** — при ошибках регистрации в логах появляются WARN с адресом SR и телом ответа.
   Помните о бэкоффах и кешировании schemaId: повторные сообщения не бьют SR повторно.

### Разворачивание Confluent Schema Registry 5.3.8 (кластерный режим)

**Ссылки:**
   - Исходники: https://github.com/confluentinc/schema-registry
   - Релиз под Kafka 2.3.1: https://github.com/confluentinc/schema-registry/tree/v5.3.8
   - README по сборке/запуску: `README.md` в теге `v5.3.8`
   - Скачивать отсюда: https://packages.confluent.io/archive/5.3/
> Мы разворачиваем **3 инстанса** на нодах `10.254.3.111`, `10.254.3.112`, `10.254.3.113`. Все конфиги идентичные. Балансировка — клиентская (в клиентах указываем все URL SR).

0. **Предпосылки**
   - Java 8 (`/usr/bin/java -version`).
   - Доступны Kafka брокеры: `10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`.
   - Открыт порт `8081/tcp` на каждой SR-нодах (firewalld).
   - На RegionServer достаточно разместить JAR `h2k-endpoint`: он уже содержит Avro/Jackson/Schema Registry-зависимости.

1. **Архитектура**
   - Версия строго 5.3.8 (совместима с Kafka 2.3.1, Java 8).
   - Схемы хранятся в Kafka-топике `_schemas` с `cleanup.policy=compact`, `replication.factor=3`.
   - Рекомендуется 3 инстанса SR (HA). Каждый инстанс без состояния (state-less), при старте дочитывает `_schemas`.

2. **Разворачивание**
   - Скачиваем
```bash
wget https://packages.confluent.io/archive/5.3/confluent-community-5.3.8-2.12.tar.gz
```
   - Разархивируем tar.gz из Confluent Platform 5.3.8 (open-source компоненты)
```bash
# на каждой ноде
cd /opt
# Поместите сюда архив Confluent 5.3.8:
# confluent-community-5.3.8-2.12.tar.gz
tar -xzf confluent-community-5.3.8-2.12.tar.gz -C /opt
# Для чистоты держим отдельную директорию под SR:
ln -sfn /opt/confluent-5.3.8 /opt/confluent-default-current
```
   - Пользователь `schema-registry` без shell.
```bash
sudo useradd -r -s /sbin/nologin -m -d /opt/confluent-default-current schema-registry || true
sudo chown -R schema-registry:schema-registry /opt/confluent-5.3.8 /opt/confluent-default-current
```

   **Итоговая структура:**
```
/opt/confluent-default-current               -> symlink на /opt/confluent-5.3.8
bin/schema-registry-start
bin/schema-registry-stop
etc/schema-registry/schema-registry.properties
share/java/schema-registry/*/*.jar
```

3. **Конфигурация `/opt/confluent-default-current/etc/schema-registry/schema-registry.properties`:**
```properties
listeners=http://0.0.0.0:8081
host.name=${HOSTNAME} # или FQDN/короткое имя, но реальное, не ${HOSTNAME}
kafkastore.bootstrap.servers=PLAINTEXT://10.254.3.111:9092,PLAINTEXT://10.254.3.112:9092,PLAINTEXT://10.254.3.113:9092
kafkastore.topic=_schemas
kafkastore.topic.replication.factor=3
kafkastore.timeout.ms=60000
compatibility.level=BACKWARD
debug=false
```
   - Все SR-инстансы используют одинаковую конфигурацию.
   - В клиентах указываем список всех SR-URL:  
     `http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081`

   - Права:
```bash
chown -R schema-registry:schema-registry /opt/confluent-default-current
```

4. **Создание `_schemas` топика (один раз):**
```bash
/opt/kafka-default-current/bin/kafka-topics.sh --bootstrap-server 10.254.3.111:9092 \
--create --topic _schemas --partitions 1 --replication-factor 3 \
--config cleanup.policy=compact \
--config min.insync.replicas=2 \
--config segment.bytes=104857600 \
--config delete.retention.ms=86400000 \
--config min.cleanable.dirty.ratio=0.1
```
   > Проверить, что RF=3, cleanup.policy=compact.

5. **Systemd unit (`/etc/systemd/system/schema-registry.service`):**
```ini
# /etc/systemd/system/schema-registry.service
[Unit]
Description=Schema Registry 5.3.8 (Confluent)
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=schema-registry
Group=schema-registry
# ВАЖНО: стартуем штатным скриптом, конфиг как аргумент
ExecStart=/opt/confluent-default-current/bin/schema-registry-start /opt/confluent-default-current/etc/schema-registry/schema-registry.properties
ExecStop=/opt/confluent-default-current/bin/schema-registry-stop
Restart=always
RestartSec=5
# Задаём переменные окружения, которые понимают скрипты 5.3.x
Environment="SCHEMA_REGISTRY_HOME=/opt/confluent-default-current"
Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx512m" "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8"
WorkingDirectory=/opt/confluent-default-current
LimitNOFILE=65536
# Журнал:
StandardOutput=journal
StandardError=journal
[Install]
WantedBy=multi-user.target
```
   - Активация:
```bash
systemctl daemon-reload
systemctl enable --now schema-registry
systemctl status schema-registry
```

6. **Проверка запуска:**
```bash
curl -s http://10.254.3.111:8081/subjects
```
Ожидаем: пустой список `[]`.

7. **Работа со схемами**
   - Регистрация схемы в SR 5.3.8 (subject: TBL_JTI_TRACE_CIS_HISTORY-value)

   На одной из нод (или с рабочего места) поместите файл схемы на диск, например: /opt/hbase-default-current/conf/avro/tbl_jti_trace_cis_history.avsc.

   - Путь к схеме
```bash
SCHEMA_FILE=/opt/hbase-default-current/conf/avro/tbl_jti_trace_cis_history.avsc
```

   - Превращаем файл в JSON-строку для API SR:
```bash
SCHEMA_JSON=$(jq -cRs . "$SCHEMA_FILE")
```

   - Регистрируем в одном из SR (в кластере одинаково на любом):
```bash
curl -sS -X POST \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data "{\"schema\":${SCHEMA_JSON}}" \
http://10.254.3.111:8081/subjects/TBL_JTI_TRACE_CIS_HISTORY-value/versions
```

   - Проверяем:
```bash
curl -s http://10.254.3.111:8081/subjects/TBL_JTI_TRACE_CIS_HISTORY-value/versions/latest | jq .
```
> Почему так: API SR требует JSON со строковым полем "schema", внутри которого лежит целиком содержимое .avsc как строка. Конструкция jq -cRs . file безопасно экранирует файл для вложения в JSON.

8. **Чтение в ClickHouse: AvroConfluent (через SR)**

   - Источник Kafka на нашем кластере (читает Confluent Avro через SR)
```sql
-- Источник из Kafka: Confluent Avro
DROP TABLE IF EXISTS stg.kafka_tbl_jti_trace_cis_history_src ON CLUSTER shardless SYNC;

CREATE TABLE IF NOT EXISTS stg.kafka_tbl_jti_trace_cis_history_src ON CLUSTER shardless
(
c String, t Int32, opd Int64,
id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),
st Nullable(Int32), ste Nullable(Int32), elr Nullable(Int32),
emd Nullable(Int64), apd Nullable(Int64), exd Nullable(Int64),
p Nullable(String), pt Nullable(Int32), o Nullable(String), pn Nullable(String), b Nullable(String),
tt Nullable(Int64), tm Nullable(Int64),
ch Array(String), j Nullable(String), pg Nullable(Int32), et Nullable(Int32), pvad Nullable(String), ag Nullable(String),
_event_ts Int64,
_delete UInt8
)
ENGINE = Kafka
SETTINGS
kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
kafka_topic_list= 'TBL_JTI_TRACE_CIS_HISTORY',
kafka_group_name= 'ch_tbl_jti_trace_cis_history',
kafka_format= 'AvroConfluent',
kafka_schema_registry_url = 'http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081',
kafka_num_consumers = 6,
kafka_skip_broken_messages = 1;
```

 - Далее — RAW и MV (типизация + TTL по _event_ts = 5 суток), на основе наших рабочих столбцов:
```sql
-- RAW-таблица приёма
DROP TABLE IF EXISTS stg.tbl_jti_trace_cis_history_raw ON CLUSTER shardless SYNC;

CREATE TABLE IF NOT EXISTS stg.tbl_jti_trace_cis_history_raw ON CLUSTER shardless
(
c String, t UInt8, opd DateTime64(3, 'UTC'),
_event_ts DateTime64(3, 'UTC'),
_delete UInt8,
id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),
st Nullable(UInt8), ste Nullable(UInt8), elr Nullable(UInt8),
emd Nullable(DateTime64(3, 'UTC')), apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
p Nullable(String), pt Nullable(UInt8), o Nullable(String), pn Nullable(String), b Nullable(String),
tt Nullable(Int64), tm Nullable(DateTime64(3, 'UTC')),
ch Array(String), j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), pvad Nullable(String), ag Nullable(String)
)
ENGINE = MergeTree
ORDER BY (c, t, opd)
TTL toDateTime(_event_ts) + INTERVAL 5 DAY DELETE;

-- Матвью: миллисекунды → DateTime64(3), приведение типов
DROP TABLE IF EXISTS stg.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER shardless SYNC;

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER shardless
TO stg.tbl_jti_trace_cis_history_raw AS
SELECT
c,
CAST(t AS UInt8) AS t,
toDateTime64(opd / 1000.0, 3, 'UTC') AS opd,
toDateTime64(_event_ts / 1000.0, 3, 'UTC') AS _event_ts,
_delete,
id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
CAST(st AS Nullable(UInt8)) AS st,
CAST(ste AS Nullable(UInt8)) AS ste,
CAST(elr AS Nullable(UInt8)) AS elr,
ifNull(toDateTime64(emd / 1000.0, 3, 'UTC'), NULL) AS emd,
ifNull(toDateTime64(apd / 1000.0, 3, 'UTC'), NULL) AS apd,
ifNull(toDateTime64(exd / 1000.0, 3, 'UTC'), NULL) AS exd,
p, CAST(pt AS Nullable(UInt8)) AS pt, o, pn, b, tt,
ifNull(toDateTime64(tm / 1000.0, 3, 'UTC'), NULL) AS tm,
ch, j, CAST(pg AS Nullable(UInt16)) AS pg, CAST(et AS Nullable(UInt8)) AS et, pvad, ag
FROM stg.kafka_tbl_jti_trace_cis_history_src;
```

9. **Бэкапы**
 - Данные SR — это Kafka-топик `_schemas`. Защищён replication factor.
 - Для DR — MirrorMaker2 или снапшоты дисков брокеров.

10. **Бест практис**
 - 3 инстанса SR в проде (1 на тесте можно).
 - `_schemas` replication factor = 3.
 - В ClickHouse/продьюсерах использовать список всех SR.
   - Мониторить доступность SR, лаг по `_schemas`, время отклика API.

> Подробнее о эксплуатации и настройке см. в `docs/runbook/operations.md` (раздел *Avro и Schema Registry*).

### Минимальная проверка в ClickHouse через SR 5.3.8

После запуска SR и регистрации схемы можно проверить чтение напрямую, не создавая матвью:

```sql
SELECT *
FROM file('/dev/null', 'AvroConfluent', 'c String, t Int32, opd Int64')
SETTINGS kafka_schema_registry_url = 'http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081',
        format_schema = 'tbl_jti_trace_cis_history.avsc'
LIMIT 0;
```

Если схема видна и корректно читается через SR 5.3.8, запрос отработает без ошибок и вернёт пустой результат со списком колонок.

## Негативные сценарии и рекомендованные тесты

- Отсутствие `.avsc` или несовместимая схема → `IllegalStateException` (см. тесты
  `GenericAvroPayloadSerializerTest`).
- Ошибка регистрации в SR (500/timeout) → `IllegalStateException` (тест
  `ConfluentAvroPayloadSerializerTest`).
- Еволюция схемы: протестируйте добавление опциональных полей и смену типов в собственных
  интеграционных тестах (потребитель должен уметь обрабатывать новые версии).

## Интеграционный поток (рекомендации)

1. **HBase → Kafka** — в бою построить стенд с тестовой таблицей, включить replication scope и
   проверить появление событий в Kafka (JSONEachRow и Avro).
2. **Kafka → ClickHouse** — из Kafka Engine читать `Avro`/`AvroConfluent` и проверить соответствие полей. Для SR укажите `kafka_schema_registry_url`.
3. **Мониторинг** — добавить метрики успешных/неуспешных регистраций, время ожидания SR и размер
   очереди schemaId. Реализация пока не входит в проект, но рекомендуется для эксплуатации.

---

Дополнительные вопросы и рецепты см. в `docs/runbook/operations.md` (раздел *Avro и Schema Registry*).
