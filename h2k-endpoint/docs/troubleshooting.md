

# Диагностика и типовые ошибки

## Общие принципы
- Все логи пишутся через log4j в systemd-journald.
- По умолчанию уровень INFO; DEBUG можно включить для пакета kz.qazmarka.h2k.endpoint.
- Сообщения исключений и диагностики пишутся на русском языке.

## Проверка конфигурации
- Убедиться, что задан h2k.kafka.bootstrap.servers.
- Проверить корректность h2k.topic.pattern и h2k.cf.list (несуществующие CF игнорируются).
- Для decode.mode=json-phoenix обязательно указать h2k.schema.path.
- При несоответствии типов в schema.json ValueCodecPhoenix бросает IllegalStateException с подробной диагностикой.

## Частые проблемы

### Нет данных в Kafka
- Проверить, что peer создан (`list_peers`).
- Проверить, что peer включён (`enable_peer`).
- Проверить, что репликация включена для таблиц (alter + REPLICATION_SCOPE).
- Убедиться, что bootstrap.servers указывают на рабочий кластер Kafka.

### Ошибки декодирования Phoenix
- ValueCodecPhoenix проверяет фиксированные типы (например, UNSIGNED_INT = 4 байта).
- Если длина не совпадает — выбрасывается IllegalStateException с сообщением «ожидалось N байт».
- Проверить schema.json, обновить при изменениях таблицы.

### Ошибки при формировании JSON
- Null в обязательных параметрах (TableName/qualifier) → NullPointerException.
- При decode.mode=json-phoenix без schema.path → ошибка конфигурации.
- При некорректной кодировке rowkey (BASE64/HEX) → ошибка парсинга RowKeySlice.

### Ошибки Kafka Producer
- Если producer не может подключиться — проверить bootstrap.servers.
- Ошибки acks/idempotence → убедиться, что профиль настроен корректно (см. peer-profiles.md).
- При переполнении буфера — увеличить h2k.producer.buffer.memory или уменьшить batch.size/linger.ms.

### Ошибки ensure.topics
- Если h2k.ensure.topics=true, но топики не создаются:
  - Проверить права у Kafka Admin.
  - Проверить h2k.topic.replication и h2k.topic.partitions.
  - Проверить h2k.admin.timeout.ms и h2k.ensure.unknown.backoff.ms.

## Инструменты быстрой диагностики
- `status 'replication'` в HBase shell — показывает статус пиров.
- Graylog/journald — для поиска ошибок по ключевым словам (IllegalStateException, NullPointerException, KafkaException).
- Логи с префиксом `kz.qazmarka.h2k.endpoint` — основной источник диагностики.

---

## Связанные документы
- [Конфигурация (все ключи)](config.md)
- [Phoenix и schema.json](phoenix.md)
- [Профили peer](peer-profiles.md)
- [HBase shell / операции](hbase.md)
- [Операции эксплуатации](operations.md)