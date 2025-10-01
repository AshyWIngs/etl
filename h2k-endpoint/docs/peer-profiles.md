# Профили peer (экспорт из `conf/add_peer_shell_*.txt`)

Документ фиксирует реальные параметры из скриптов `conf/add_peer_shell_fast.txt`, `conf/add_peer_shell_balanced.txt`, `conf/add_peer_shell_reliable.txt`.  
Используйте его как «матрицу истинности» при ревью или подготовке собственных профилей.

---

## Сводная таблица ключевых отличий

| Профиль | Целевой сценарий | Формат payload | Avro режим | `acks` | `enable.idempotence` | `max.in.flight` | `linger.ms` | `batch.size` | `compression.type` |
|---------|------------------|----------------|-----------|--------|----------------------|-----------------|-------------|--------------|--------------------|
| **FAST** | Максимальная скорость, допускаем дубликаты | `json-each-row` (Avro опционально) | — | `1` | `false` | `5` | `100` | `524288` (512 KiB) | `lz4` |
| **BALANCED** | Прод формат с Avro и умеренным батчингом | `avro-binary` | `generic` (`conf/avro/*.avsc`) | `all` | `true` | `5` | `100` | `524288` | `lz4` |
| **RELIABLE** | Максимальная надёжность и порядок | `json-each-row` (Avro опционально) | — | `all` | `true` | `1` | `50` | `65536` (64 KiB) | `snappy` |

Дополнительные общие настройки (равны во всех профилях):

- `h2k.kafka.bootstrap.servers=10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092`
- `h2k.topic.pattern=${table}`
- `h2k.cf.list=d`
- `h2k.decode.mode=phoenix-avro`
- `h2k.schema.path=/opt/hbase-default-current/conf/schema.json`
- `h2k.json.serialize.nulls=false`
- `h2k.rowkey.encoding=BASE64`
- `h2k.ensure.topics=true`, `h2k.topic.partitions=12`, `h2k.topic.replication=3`
- `h2k.producer.await.every=500`, `h2k.producer.await.timeout.ms=180000` (FAST) / `300000` (BALANCED/RELIABLE)
- `h2k.producer.buffer.memory=268435456`, `h2k.producer.max.request.size=2097152`

---

## FAST: максимальная скорость

```ruby
add_peer 'h2k_fast', {
  'ENDPOINT_CLASSNAME' => 'kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint',
  'CONFIG' => {
    'h2k.payload.format'              => 'json-each-row',   # Avro при необходимости раскомментировать
    'h2k.producer.enable.idempotence' => 'false',
    'h2k.producer.acks'               => '1',
    'h2k.producer.max.in.flight'      => '5',
    'h2k.producer.retries'            => '10',
    'h2k.producer.request.timeout.ms' => '30000',
    'h2k.producer.delivery.timeout.ms'=> '90000',
    'h2k.producer.linger.ms'          => '100',
    'h2k.producer.batch.size'         => '524288',
    'h2k.producer.compression.type'   => 'lz4'
  }
}
```

**Назначение:** ingestion, когда кратковременные дубликаты допустимы и важна минимальная задержка подтверждения.  
**Комментарий:** если включаете Avro — добавьте `h2k.avro.mode=generic` и `h2k.avro.schema.dir` (или `confluent` + SR параметры).

---

## BALANCED: Avro по умолчанию

```ruby
add_peer 'h2k_balanced', {
  'CONFIG' => {
    'h2k.payload.format'              => 'avro-binary',
    'h2k.avro.mode'                   => 'generic',
    'h2k.avro.schema.dir'             => '/opt/hbase-default-current/conf/avro',
    'h2k.producer.enable.idempotence' => 'true',
    'h2k.producer.acks'               => 'all',
    'h2k.producer.max.in.flight'      => '5',
    'h2k.producer.retries'            => '2147483647',
    'h2k.producer.request.timeout.ms' => '120000',
    'h2k.producer.delivery.timeout.ms'=> '300000',
    'h2k.producer.linger.ms'          => '100',
    'h2k.producer.batch.size'         => '524288',
    'h2k.producer.compression.type'   => 'lz4'
  }
}
```

**Назначение:** основной продакшен-профиль: Avro, идемпотентность и сохранение порядка при `max.in.flight ≤ 5`.  
**SR (Confluent):** замените блок `avro.mode` на `confluent`, добавьте `h2k.avro.sr.urls` и опциональные ключи авторизации.

---

## RELIABLE: жёсткий порядок и отсутствие дублей

```ruby
add_peer 'h2k_reliable', {
  'CONFIG' => {
    'h2k.payload.format'              => 'json-each-row',   # Avro включается аналогично fast
    'h2k.producer.enable.idempotence' => 'true',
    'h2k.producer.acks'               => 'all',
    'h2k.producer.max.in.flight'      => '1',
    'h2k.producer.retries'            => '2147483647',
    'h2k.producer.request.timeout.ms' => '120000',
    'h2k.producer.delivery.timeout.ms'=> '300000',
    'h2k.producer.linger.ms'          => '50',
    'h2k.producer.batch.size'         => '65536',
    'h2k.producer.compression.type'   => 'snappy'
  }
}
```

**Назначение:** поток с жёсткими требованиями к порядку и повторяемости (финансы, бухгалтерия).  
**Совет:** при увеличении нагрузки поднимайте только `linger.ms`/`batch.size`, но не `max.in.flight` — иначе нарушится порядок.

---

## Где искать дополнительные параметры

- Детальные комментарии и подсказки — прямо в `conf/add_peer_shell_*.txt` (разделы «Быстрые подсказки»).  
- Полное описание ключей `h2k.*` — в `docs/config.md`.  
- Про форматы данных и работу со схемами — `docs/avro.md` и `README.md` (раздел «Поддержка форматов сообщений»).
