# Интеграционные проверки Avro

## Стенд и подготовка

1. **HBase**: создайте таблицу `DEFAULT:T_AVRO`, включите репликацию CF (`REPLICATION_SCOPE=1`).
2. **Kafka**: убедитесь, что тема доступна или включен `h2k.ensure.topics`.
3. **Schema Registry** (для Confluent): запустите SR 5.3.x.
4. **Потребитель**: ClickHouse или другой consumer (Kafka Engine).

## Режим generic (локальные `.avsc`)

```properties
h2k.payload.format=avro-binary
h2k.avro.mode=generic
h2k.avro.schema.dir=/opt/hbase/conf/avro
```

- Положите `t_avro.avsc` в указанный каталог.
- Отправьте данные в HBase → проверьте Kafka (`kafka-console-consumer`).
- ClickHouse: Kafka Engine + `FORMAT Avro` (указывает путь к `.avsc`).

## Режим Confluent

```properties
h2k.payload.format=avro-binary
h2k.avro.mode=confluent
h2k.avro.schema.dir=/opt/hbase/conf/avro
h2k.avro.sr.urls=http://sr1:8081,http://sr2:8081
h2k.avro.sr.auth.basic.username=svc-hbase
h2k.avro.sr.auth.basic.password=***
h2k.avro.subject.strategy=table
```

- После первого события проверьте `curl http://sr1:8081/subjects` — должен появиться subject.
- Kafka consumer: `FORMAT AvroConfluent` в ClickHouse с `kafka_schema_registry_url`.

## Негативные сценарии

- Нет `.avsc` → ожидаем `IllegalStateException` в логах, peer продолжает работу.
- SR отвечает 500/timeout → WARN в логах; убедитесь, что происходит повторная попытка.
- Изменение схемы: обновите `.avsc`, перезапустите peer (disable/enable), проверьте новую версию в SR.

## Мониторинг

- Включайте DEBUG для пакета `kz.qazmarka.h2k` на время тестов.
- Снимайте метрики BatchSender и TopicEnsurer.

Дополнительные подробности — в `docs/avro.md`.
