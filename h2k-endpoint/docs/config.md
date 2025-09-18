# Документация по конфигурации `h2k.*` (точная спецификация)

Ниже — **строго соответствующие коду** (`src/main/java/kz/qazmarka/h2k/config/H2kConfig.java`) и примерам (`conf/add_peer_shell_*.txt`, `README.md`) ключи `h2k.*`. Без домыслов.

> Приоритет источников: **CONFIG в peer** ⟶ `hbase-site.xml`. Любой ключ из `h2k.*`, заданный в peer, перекрывает одноимённый ключ в `hbase-site.xml`.

---

## Содержание
- [Обязательные и базовые ключи](#обязательные-и-базовые-ключи)
- [Топики Kafka](#топики-kafka)
- [CF (Column Families)](#cf-column-families)
- [Декодирование и схема Phoenix](#декодирование-и-схема-phoenix)
- [Метаполя и формат rowkey](#метаполя-и-формат-rowkey)
- [Подсказки ёмкости](#подсказки-ёмкости)
- [Соль rowkey (Phoenix)](#соль-rowkey-phoenix)
- [Автосоздание/администрирование тем](#автосозданиеадминистрирование-тем)
- [Kafka Producer: pass-through и спец-ключи](#kafka-producer-pass-through-и-спец-ключи)
- [Профильные примеры из peer-скриптов](#профильные-примеры-из-peer-скриптов)

---

## Обязательные и базовые ключи

| Ключ | Назначение | Допустимые значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.kafka.bootstrap.servers`** | Список брокеров Kafka | `host:port[,host2:port2]` | `10.254.3.111:9092,10.254.3.112:9092` |

---

## Топики Kafka

| Ключ | Назначение | Допустимые значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.topic.pattern`** | Шаблон имени топика | Подставляются плейсхолдеры: `${table}`, `${namespace}`, `${qualifier}`. **Для default‑namespace префикс не пишем.** Дефолт: `${table}` | `${table}` |
| **`h2k.topic.max.length`** | Лимит длины имени топика (символов) | По коду дефолт = `249` | `249` |
| **`h2k.topic.partitions`** | Число разделов при автосоздании | Целое (без дефолта — используется брокерный) | `12` |
| **`h2k.topic.replication`** | Фактор репликации при автосоздании | Целое (без дефолта — используется брокерный) | `3` |
| **`h2k.topic.config.*`** | Доп. конфиги темы | Любые валидные свойства темы Kafka | `h2k.topic.config.cleanup.policy=compact` |

---

## CF (Column Families)

| Ключ | Назначение | Формат | Пример |
|---|---|---|---|
| **`h2k.cf.list`** | Список CF для экспорта | CSV **без пробелов**. Несуществующие CF игнорируются. | `d,0,DOCUMENTS` |

> Поля **PK** из rowkey всегда инъектируются в payload автоматически; добавлять их в `cf.list` не требуется.

---

## Декодирование и схема Phoenix

| Ключ | Назначение | Значения / Требования | Пример |
|---|---|---|---|
| **`h2k.decode.mode`** | Режим декодирования значений | `simple` \| `json-phoenix` | `json-phoenix` |
| **`h2k.schema.path`** | Путь к `schema.json` (Phoenix) | **Обязателен**, если `h2k.decode.mode=json-phoenix` | `/opt/hbase-default-current/conf/schema.json` |
| **`h2k.json.serialize.nulls`** | Сериализовать `null` в JSON | `true`/`false` (дефолт: `false`) | `false` |

---

## Метаполя и формат rowkey

| Ключ | Назначение | Значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.payload.include.meta`** | Добавлять базовые метаполя | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.payload.include.meta.wal`** | Добавлять WAL‑метаполя | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.payload.include.rowkey`** | Включать `_rowkey` в payload | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.rowkey.encoding`** | Кодировка `_rowkey` | `BASE64` \| `HEX` | `BASE64` |

---

## Подсказки ёмкости

| Ключ | Назначение | Формат | Пример |
|---|---|---|---|
| **`h2k.capacity.hints`** | Оценка «максимума не‑null полей» для таблиц | `TABLE=keys[,NS:TABLE=keys2,...]` (для DEFAULT‑NS имя без `DEFAULT`) | `TBL_JTI_TRACE_CIS_HISTORY=32` |
| **`h2k.capacity.hint.<TABLE>`** | Точечная подсказка для одной таблицы | `<int>` | `h2k.capacity.hint.RECEIPT=18` |

> Подсказки используются для предразмеривания структур в горячем пути (меньше аллокаций/GC). Если включены метаполя, добавляйте к «ключам» соответствующие константы (см. `docs/capacity.md`).

---

## Соль rowkey (Phoenix)

| Ключ | Назначение | Формат | Пример |
|---|---|---|---|
| **`h2k.salt.map`** | Сопоставление таблиц и числа байт соли в начале rowkey | `TABLE=bytes[,NS:TABLE=bytes2][,default=0]` | `TBL_JTI_TRACE_CIS_HISTORY=1,default=0` |

---

## Автосоздание/администрирование тем

| Ключ | Назначение | Значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.ensure.topics`** | Создавать тему автоматически | `true`/`false` | `true` |
| **`h2k.ensure.increase.partitions`** | Разрешить увеличение партиций | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.ensure.diff.configs`** | Применять отличающиеся конфиги темы | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.admin.timeout.ms`** | Таймаут операций админ‑клиента | `int` (ms) | `30000` |
| **`h2k.admin.client.id`** | ClientId для админ‑клиента | Строка | `h2k-admin` |
| **`h2k.ensure.unknown.backoff.ms`** | Бэкофф при «неизвестной теме» | `int` (ms) | `5000` |

---

## Kafka Producer: pass-through и спец-ключи

**Pass‑through:** все ключи с префиксом **`h2k.producer.`** копируются в конфигурацию Kafka Producer **без изменений имени** (например, `h2k.producer.acks`, `h2k.producer.linger.ms`, `h2k.producer.compression.type`, и т.д.).

**Спец‑ключи проекта:**

| Ключ | Что делает | Примечание |
|---|---|---|
| **`h2k.producer.max.in.flight`** | Упрощённая запись для Kafka‑ключа `max.in.flight.requests.per.connection` | Используйте `1` в строгом профиле (RELIABLE); см. README и peer‑скрипты |
| **`h2k.producer.await.every`** | Каждые N записей ждать ACK (бэкпрешер) | Не Kafka‑ключ; внутренняя логика endpoint |
| **`h2k.producer.await.timeout.ms`** | Таймаут ожидания ACK | Не Kafka‑ключ; внутренняя логика endpoint |
| **`h2k.producer.batch.counters.enabled`** | Счётчики батчей | Не Kafka‑ключ |
| **`h2k.producer.batch.debug.on.failure`** | Доп. диагностика при сбоях | Не Kafka‑ключ |

> Полную матрицу рекомендованных настроек по профилям см. в README и `docs/peer-profiles.md`.

---

## Профильные примеры из peer-скриптов

**BALANCED (фрагмент из `conf/add_peer_shell_balanced.txt`):**
```ruby
'h2k.kafka.bootstrap.servers'   => '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
'h2k.topic.pattern'             => '${table}',
'h2k.cf.list'                   => 'd',
'h2k.decode.mode'              => 'json-phoenix',
'h2k.schema.path'              => '/opt/hbase-default-current/conf/schema.json',
'h2k.json.serialize.nulls'     => 'false',
'h2k.payload.include.meta'     => 'false',
'h2k.payload.include.meta.wal' => 'false',
'h2k.payload.include.rowkey'   => 'false',
'h2k.rowkey.encoding'          => 'BASE64',
'h2k.salt.map'                 => 'TBL_JTI_TRACE_CIS_HISTORY=1',
'h2k.capacity.hints'           => 'TBL_JTI_TRACE_CIS_HISTORY=32',
'h2k.ensure.topics'            => 'true',
'h2k.topic.partitions'         => '12',
'h2k.topic.replication'        => '3',
'h2k.admin.timeout.ms'         => '30000',
'h2k.ensure.unknown.backoff.ms'=> '5000'
```

**FAST / RELIABLE** — см. соответствующие файлы в `conf/` и README для матрицы acks/idempotence/max.in.flight/компрессии.

---

## Примечания
- Имена таблиц/CF — латиница. Значения могут быть на русском/казахском — без порчи при доставке.
- Все ключи из этого документа — **ровно те**, что используются кодом (см. `H2kConfig`), peer‑скриптами и README.