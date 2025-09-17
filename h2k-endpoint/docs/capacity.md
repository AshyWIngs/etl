# Capacity Hints и метаданные (QA)

Кратко: `h2k.capacity.hints` подсказывает `PayloadBuilder`, какой стартовой ёмкости `LinkedHashMap` выделять под JSON одной строки. Верная ёмкость уменьшает ребилды карты и мусор в GC на горячем пути.

---

## Что именно считать в hint

Итоговое число ключей в JSON для «типичной» строки складывается из:
```
CAPACITY_HINT =
  N(CF)                // колонки из выбранных CF, которые обычно не NULL
+ N(PK)                // колонки первичного ключа — ВСЕГДА присутствуют в JSON
+ (include.meta      ? 8 : 0)   // базовые метаполя
+ (include.meta.wal  ? 2 : 0)   // метаполя WAL
+ (include.rowkey    ? 1 : 0)   // сериализованный rowkey
```

### Почему так
- **CF**: считаем только те колонки целевых CF, которые реально приходят не‑NULL в типичном кейсе (лучше ориентироваться на **P95**, а не на абсолютный максимум).
- **PK всегда в JSON** — независимо от `h2k.cf.list`.
- **Метаполя** регулируются флагами и добавляют фиксированное число ключей (см. ниже).
- **Salt (Phoenix)** не добавляет ключей в JSON — это префикс в rowkey; он влияет на парсинг, а не на количество полей.

---

## Метаполя: состав и когда появляются

### Если `h2k.payload.include.meta = true` (базовые, **8** ключей)
- `_table` — имя таблицы в топике;
- `_namespace` — HBase namespace;
- `_qualifier` — имя таблицы без префикса namespace;
- `_cf` — CF, откуда были изменения;
- `_cells_total` — всего ячеек в WALEdit;
- `_cells_cf` — ячеек по выбранному CF;
- `event_version` — максимальная метка времени среди ячеек CF;
- `delete` — `true`, если в партии был delete‑маркер по CF.

### Если `h2k.payload.include.meta.wal = true` (WAL, **2** ключа)
- `_wal_seq` — sequence id записи в WAL (long);
- `_wal_write_time` — время записи в WAL в миллисекундах epoch.

### Если `h2k.payload.include.rowkey = true` (строго **1** ключ)
- `_rowkey` — строковый rowkey в кодировке из `h2k.rowkey.encoding` (`BASE64` или `HEX`).

> **Важно:** флаги независимые. Вы можете включать только WAL‑метаданные, оставив базовые `include.meta=false` — тогда добавятся ровно **2** поля.

---

## QA‑пример: `TBL_JTI_TRACE_CIS_HISTORY`

По `schema.json` и факту на QA:
- Всего колонок в таблице (включая `PK: c, t, opd`) — **32** → это и есть базовый JSON без метаполей.
- `N(PK) = 3` (всегда в JSON, даже если CF не содержит этих колонок явно).

Расчёты:

- **Текущие дефолты** (`include.meta=false`, `include.meta.wal=false`, `include.rowkey=false`):  
  `hint = 32`.

- **Если включить только WAL‑метаданные** (`include.meta.wal=true`):  
  `hint = 32 + 2 = 34`.

- **Если включить базовые метаполя и rowkey** (`include.meta=true`, `include.rowkey=true`):  
  `hint = 32 + 8 + 1 = 41`.

- **Полный набор (meta + wal + rowkey)**:  
  `hint = 32 + 8 + 2 + 1 = 43`.

> На QA мы используем:  
> • профиль **balanced** без метаполей → `TBL_JTI_TRACE_CIS_HISTORY=32`;  
> • при диагностике с WAL‑метаданными → `TBL_JTI_TRACE_CIS_HISTORY=34`.

---

## Где задавать значения

### В peer‑конфиге (имеет приоритет)
```HBase shell
add_peer 'h2k_balanced',
  {
    'ENDPOINT_CLASSNAME' => 'kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint',
    'CONFIG' => {
      'h2k.kafka.bootstrap.servers' => '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
      'h2k.topic.pattern'           => '${table}',
      'h2k.cf.list'                 => 'd',

      # Capacity Hints для QA:
      'h2k.capacity.hints'          => 'TBL_JTI_TRACE_CIS_HISTORY=32'
      # при включении WAL‑метаданных:
      # 'h2k.capacity.hints'        => 'TBL_JTI_TRACE_CIS_HISTORY=34'
    }
  }
```

### В `hbase-site.xml`
```xml
<property>
  <name>h2k.capacity.hints</name>
  <value>TBL_JTI_TRACE_CIS_HISTORY=32</value>
</property>
```
Для таблиц из `DEFAULT` namespace используйте просто имя таблицы (без `DEFAULT.`).

---

## О соли (`h2k.salt.map`) — когда и зачем

Если таблица Phoenix «солёная» (`SALT_BUCKETS > 0`), первый байт rowkey — это соль. Укажите это, чтобы корректно извлекать PK из rowkey:
```properties
h2k.salt.map = TBL_JTI_TRACE_CIS_HISTORY=1
```
- Значение — длина префикса соли в **байтах** (у Phoenix — всегда 1).
- Это **не влияет** на `CAPACITY_HINT`, только на правильный парсинг rowkey и, как следствие, корректную инъекцию PK в JSON.

---

## Как быстро проверить (на реальном потоке QA)

Подсчитать реальное число ключей в сообщениях (выборка) и сравнить с выбранным hint:

```bash
kafka-console-consumer.sh \
  --bootstrap-server 10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092 \
  --topic TBL_JTI_TRACE_CIS_HISTORY --from-beginning --max-messages 200 \
| jq -c 'keys|length' \
| sort -n | uniq -c
```

- Ориентируйтесь на **P95–P99** по числу ключей, а не на единичные пики.
- Если часто видите значение больше hint — поднимите hint на ближайшую «круглую» величину.

---

## Чек‑лист

- [ ] Указан hint для «широких» таблиц (обычно ≥ 16 ключей).
- [ ] Включены в расчёт `PK` (они всегда в JSON).
- [ ] Учтены флаги метаполей: `include.meta`, `include.meta.wal`, `include.rowkey`.
- [ ] Для salted таблиц задан корректный `h2k.salt.map`.
- [ ] Значение ориентировано на **P95**, а не на max.
- [ ] Значение продублировано в peer (или в `hbase-site.xml`) и проверено через `get_peer_config`.

---

Корректные `h2k.capacity.hints` снижают аллокации и паузы GC, повышая стабильность и скорость репликации на QA/PROD.