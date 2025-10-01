

# Диагностика и типовые ошибки

## Общие принципы
- Все логи пишутся через log4j в systemd-journald.
- По умолчанию уровень INFO; DEBUG можно включить для пакета kz.qazmarka.h2k.endpoint.
- Сообщения исключений и диагностики пишутся на русском языке.

## Проверка конфигурации
- Убедиться, что задан h2k.kafka.bootstrap.servers.
- Проверить корректность h2k.topic.pattern и h2k.cf.list (несуществующие CF игнорируются).
- Для decode.mode=phoenix-avro рекомендуется указать h2k.schema.path как фолбэк; для json-phoenix этот ключ обязателен.
- При несоответствии типов в schema.json `PhoenixColumnTypeRegistry` логирует WARN и использует VARCHAR, а `ValueCodecPhoenix` бросает `IllegalStateException` при декодировании фиксированных типов.

## Частые проблемы

### Нет данных в Kafka
- Проверить, что peer создан (`list_peers`).
- Проверить, что peer включён (`enable_peer`).
- Проверить, что репликация включена для таблиц (alter + REPLICATION_SCOPE).
- Убедиться, что bootstrap.servers указывают на рабочий кластер Kafka.
- Если используется фильтр `h2k.cf.list`, убедитесь, что ключ задан явно и список CF соответствует реальному регистру имён.

### Ошибки декодирования Phoenix
- ValueCodecPhoenix проверяет фиксированные типы (например, UNSIGNED_INT = 4 байта).
- Если длина не совпадает — выбрасывается IllegalStateException с сообщением «ожидалось N байт».
- При неизвестном типе смотрите WARN от PhoenixColumnTypeRegistry (тип будет принят как VARCHAR).
- Для phoenix-avro проверить `.avsc` (атрибуты `h2k.phoenixType`/`h2k.pk`) и, при необходимости, schema.json для фолбэка.

### Ошибки при формировании JSON
- Null в обязательных параметрах (TableName/qualifier) → NullPointerException.
- При decode.mode=json-phoenix без schema.path → ошибка конфигурации.
- При decode.mode=phoenix-avro без `.avsc` и без schema.json → декодер не сможет инициализироваться.
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
  - В логах TopicEnsureService ищите метрики `ensure.*`, `exists.*`, `create.*` — они показываются при DEBUG и
    доступны через `TopicEnsurer#getMetrics()`. Поле `unknown.backoff.size` отражает размер очереди ожидания без
    лишних копий.
  - Для детального анализа backoff воспользуйтесь `TopicEnsurer#getBackoffSnapshot()`: метод возвращает
    неизменяемую карту `topic → миллисекунды до повторной попытки` (отрицательные значения уже обнуляются).

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
