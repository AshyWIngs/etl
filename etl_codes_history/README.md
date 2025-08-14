# ETL: Phoenix/HBase → ClickHouse (PG journal, native 9000)

Инкрементальный перенос из Phoenix (HBase) в ClickHouse **напрямую** (без CSV),
с журналом запусков и `watermark` в PostgreSQL. Вставки идут через **native 9000**
сжатый транспорт (lz4/zstd, cityhash).

---
## Что делает
- Читает окно из Phoenix (`TBL_JTI_TRACE_CIS_HISTORY`) по колонке времени `opd`.
- Буферизует и пишет строки **сразу** в ClickHouse RAW (Distributed) чанками.
- Ведёт журнал запусков в PostgreSQL: `planned → running → ok/error/skipped` + `watermark`.
- Гарантирует **один активный запуск**: advisory‑lock + частичный UNIQUE‑индекс.
- Автоматически санитизирует «висячие» записи журнала перед стартом.
- После загрузки — выполняет **дедуп/публикацию партиций** в «чистую» таблицу через `REPLACE PARTITION`.
- Логирует **метрики дедупа** по этапам и пишет их heartbeat‑ами в PG.

---
## Новое / ключевые изменения
- Клиент ClickHouse (native 9000) с **failover по списку хостов**, health‑check `SELECT 1`,
  авто‑`reconnect()` и **одним повтором** при `UnexpectedPacketFromServerError` (в т.ч. EndOfStream).
- **Чёткая схема таблиц в CH** и выравнивание имён с ENV/DDL:
  - RAW локальная: `stg.daily_codes_history_raw`
  - RAW Distributed: `stg.daily_codes_history_raw_all` ← **источник для дедупа**
  - BUF (для дедупа, без TTL): `stg.daily_codes_history_dedup_buf`
  - CLEAN локальная: `stg.daily_codes_history`
  - CLEAN Distributed: `stg.daily_codes_history_all`
- **ENV приведён к единому виду**: используем `CH_RAW_TABLE`, `CH_CLEAN_TABLE`,
  `CH_CLEAN_ALL_TABLE`, `CH_DEDUP_BUF_TABLE`; список колонок — `MAIN_COLUMNS`
  (есть fallback на старое имя `HBASE_MAIN_COLUMNS`).
- **Грануляция по слайсам** из .env: `STEP_MIN=10` + окно Phoenix сдвигается на
  `PHX_QUERY_SHIFT_MINUTES`, первый слайс имеет `PHX_QUERY_OVERLAP_MINUTES`, справа — `PHX_QUERY_LAG_MINUTES`.
- Автокаденс публикаций: `PUBLISH_EVERY_MINUTES` **или** `PUBLISH_EVERY_SLICES`; финальная публикация — `ALWAYS_PUBLISH_AT_END=1`.
- Под‑журнал для дедупа: отдельный `process_name: "<PROCESS_NAME>:dedup"` на каждую публикацию.

---
## Как это работает (поток данных)
1. Интервал `[--since, --until)` режется на «слайсы» длиной `STEP_MIN`.
2. Для каждого слайда строится окно **Phoenix** с учётом:
   - `PHX_QUERY_SHIFT_MINUTES` (технический сдвиг),
   - `PHX_QUERY_OVERLAP_MINUTES` (захлёст слева на первом слайсе, опционально на всех),
   - `PHX_QUERY_LAG_MINUTES` (лаг справа, чтобы не брать «кипящее»).
3. Чтение из Phoenix батчами (`PHX_FETCHMANY_SIZE`), локальный дедуп в рамках запуска по ключу `(c, t, opd)`.
4. Приведение типов под DDL (`DateTime64(3)` как `datetime` без TZ, микросекунды → миллисекунды).
5. Пакетные **INSERT VALUES** в `CH_RAW_TABLE` (обычно `stg.daily_codes_history_raw_all`).
6. Накопление затронутых партиций (`toYYYYMMDD(opd)`). По каденсу выполняется публикация:
   - `ALTER TABLE …_dedup_buf DROP PARTITION p` (очистка буфера),
   - `INSERT INTO …_dedup_buf SELECT c, t, opd, argMax(<все прочие>, ingested_at)… FROM RAW_ALL WHERE part=p GROUP BY c,t,opd`,
   - `ALTER TABLE CLEAN REPLACE PARTITION p FROM …_dedup_buf`,
   - `ALTER TABLE …_dedup_buf DROP PARTITION p`.

---
## DDL (ClickHouse)
Готовый файл: `ddl/stg_daily_codes_history.sql`.
Основные объекты (локальные и Distributed) — см. выше. TTL у RAW по `ingested_at`,
у CLEAN можно задавать бизнес‑TTL (пример в DDL: `TTL toDateTime(opd) + INTERVAL 5 DAY DELETE`).

---
## Быстрый старт
```bash
# 1) Python 3.12, окружение
python3.12 -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -r requirements.txt

# 2) ClickHouse DDL
clickhouse-client -n --queries-file=ddl/stg_daily_codes_history.sql

# 3) ENV
cp .env.example .env   # отредактируйте доступы/хосты при необходимости

# 4) Запуск
python -m scripts.codes_history_etl \
  --since "2025-08-08T00:00:00Z" \
  --until "2025-08-09T00:00:00Z"
```
PG‑таблицы журнала создаются автоматически (см. `journal.ensure()`).

---
## ENV (актуальная схема)
```ini
# --- PostgreSQL (журнал) ---
PG_DSN=postgresql://etl_user:password@10.254.3.91:5432/etl_database?connect_timeout=5&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5
JOURNAL_TABLE=public.inc_processing
PROCESS_NAME=codes_history_increment
JOURNAL_RETENTION_DAYS=1

# --- Phoenix ---
PQS_URL=http://10.254.3.112:8765
HBASE_MAIN_TABLE=TBL_JTI_TRACE_CIS_HISTORY
HBASE_MAIN_TS_COLUMN=opd
MAIN_COLUMNS=c,t,opd,id,did,rid,rinn,rn,sid,sinn,sn,gt,prid,st,ste,elr,emd,apd,exd,p,pt,o,pn,b,tt,tm,ch,j,pg,et,pvad,ag
PHX_FETCHMANY_SIZE=5000
PHX_TS_UNITS=timestamp

# --- Грануляция и PQS окна ---
STEP_MIN=10
PHX_QUERY_SHIFT_MINUTES=-300
PHX_QUERY_OVERLAP_MINUTES=5
PHX_OVERLAP_ONLY_FIRST_SLICE=1
PHX_QUERY_LAG_MINUTES=2

# --- ClickHouse (native:9000) ---
CH_HOSTS=10.254.3.111,10.254.3.112,10.254.3.113,10.254.3.114
CH_PORT=9000
CH_DB=stg
CH_USER=default
CH_PASSWORD=
# Таблицы:
CH_RAW_TABLE=stg.daily_codes_history_raw_all
CH_CLEAN_TABLE=stg.daily_codes_history
CH_CLEAN_ALL_TABLE=stg.daily_codes_history_all
CH_DEDUP_BUF_TABLE=stg.daily_codes_history_dedup_buf

# --- ClickHouse insert ---
CH_INSERT_BATCH=20000
CH_INSERT_MAX_RETRIES=1

# --- Каденс публикаций ---
PUBLISH_EVERY_MINUTES=60
PUBLISH_EVERY_SLICES=6
ALWAYS_PUBLISH_AT_END=1
```

---
## Наблюдаемость
В лог и в `inc_processing.details` (heartbeat) попадают метрики вида:
```json
{"stage":"drop_buf","part":20250115,"ms":64}
{"stage":"insert_dedup","part":20250115,"ms":3748}
{"stage":"replace_clean","part":20250115,"ms":380}
{"stage":"drop_buf_cleanup","part":20250115,"ms":47}
{"dedup_part_total_ms":{"20250115":4284}}
```

---
## Журнал и надёжность
- **Эксклюзивность**: advisory‑lock по `process_name` + частичный UNIQUE‑индекс
  `WHERE ts_end IS NULL AND status IN ('planned','running')`.
- **Автосанация** перед стартом: `sanitize_stale()`
  - planned > TTL → `skipped`,
  - running с протухшим heartbeat → `error`,
  - (опционально) running старше жёсткого TTL → `error`.
- **Под‑журнал публикации**: имя процесса `<PROCESS_NAME>:dedup`, интервалы строятся по
  множеству затронутых партиций.

---
## Тонкая настройка
- Phoenix: `PHX_FETCHMANY_SIZE`, `PHX_TS_UNITS`, сдвиги окна (shift/overlap/lag).
- ClickHouse: включено сжатие транспорта, для него нужны `lz4`, `zstd`, `clickhouse-cityhash`.
- Дедуп: `argMax(col, ingested_at)` по всем неключевым столбцам; ключ — `(c,t,opd)`.

---
## Чек‑лист согласованности (ENV ↔ DDL ↔ код)
1. Таблицы в CH существуют и совпадают по именам:
   - `stg.daily_codes_history_raw`, `stg.daily_codes_history_raw_all`,
   - `stg.daily_codes_history_dedup_buf`,
   - `stg.daily_codes_history`, `stg.daily_codes_history_all`.
   Пример проверки:
   ```sql
   SHOW TABLES FROM stg LIKE 'daily_codes_history%';
   ```
2. В `.env` нет устаревших переменных (`CH_TABLE`, `HBASE_MAIN_COLUMNS`).
3. В логах слайсы соответствуют `STEP_MIN` (и выводится shift/overlap/lag).
4. Ошибки CH по сети единичны и гаснут на повторе — иначе проверьте сеть/кластер.

---
## Частые проблемы
- **`Could not find table: daily_codes_history_buf`** — неверное имя буфера.
  Должно быть `CH_DEDUP_BUF_TABLE=stg.daily_codes_history_dedup_buf` и DDL из `ddl/` применён.
- **`UniqueViolation inc_processing_active_one_uq_idx`** — уже есть активный запуск.
  Подождать завершения или запустить снова: перед стартом выполнится `sanitize_stale()`.
- **`can't subtract offset-naive and offset-aware datetimes`** — передавайте `--since/--until` с TZ или без TZ **оба**.
- **`TTL ... should have DateTime or Date, but has DateTime64`** — используйте `toDateTime(opd)` в TTL (см. DDL).
- **`No module named 'zstd'`** — `pip install zstd`.
- **`Package clickhouse-cityhash is required to use compression`** —
  `pip install clickhouse-cityhash==1.0.2.4`.
- **`clickhouse_driver.errors.UnexpectedPacketFromServerError`** — клиент делает reconnect+повтор; если часто, проверьте сеть.

---
## Структура проекта
- `scripts/codes_history_etl.py` — основной ETL и дедуп/публикация.
- `scripts/journal.py` — журнал в PG / watermark / эксклюзив.
- `scripts/db/phoenix_client.py` — клиент PQS (итерация по окну, fetchmany).
- `scripts/db/clickhouse_client.py` — ClickHouse native (failover, retries, reconnect).
- `scripts/config.py` — загрузка ENV, дефолты, совместимость имён переменных.
- `ddl/stg_daily_codes_history.sql` — все таблицы RAW/CLEAN/BUF (+ Distributed).

---
## Лицензия / Авторство
Внутренний служебный проект.