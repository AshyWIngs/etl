# ETL: Phoenix/HBase → ClickHouse (PG‑журнал, native 9000)

Инкрементальный перенос из **Phoenix (HBase)** в **ClickHouse** напрямую (без CSV)
с журналом запусков и `watermark` в PostgreSQL. Вставки идут через **native 9000**
со сжатием (lz4/zstd, cityhash) и failover по списку хостов.

---

## Что делает
- Читает окно из Phoenix (`TBL_JTI_TRACE_CIS_HISTORY`) по колонке времени `opd`.
- Безопасно и **быстро** выгружает данные батчами (`fetchmany`) с «умной» адаптивной нарезкой окон.
- Пишет строки **сразу** в ClickHouse RAW (обычно Distributed) чанками.
- Ведёт журнал запусков в PostgreSQL: `planned → running → ok/error/skipped` + хранит `watermark`.
- Гарантирует **один активный запуск**: advisory‑lock + частичный UNIQUE‑индекс.
- Перед стартом автоматически санитизирует «висячие» записи (`planned/running`) по TTL.
- Выполняет **дедуп/публикацию** партиций в «чистую» таблицу через `REPLACE PARTITION`.
- Логирует метрики дедупа и пишет их heartbeat‑ами в PG (для наблюдаемости и алёртов).

---

## Что нового / ключевые изменения
- **PhoenixClient**:
  - адаптивная вытяжка: начальная нарезка окна на сетке UTC + рекурсивный сплит и экспоненциальный бэк‑офф при перегрузке PQS;
  - TCP‑проба соединения до первого запроса (тонкая настройка таймаута);
  - вся «защита от перегрузки» и работа с окнами теперь *в клиенте*, чтобы не было дублирования логики.
- **ClickHouse (native 9000)**:
  - failover по списку узлов (`CH_HOSTS`), health‑check `SELECT 1`, `reconnect()` и один повтор при `UnexpectedPacketFromServerError` (в т.ч. EndOfStream).
- **ENV унифицирован**: используем `CH_RAW_TABLE`, `CH_CLEAN_TABLE`, `CH_CLEAN_ALL_TABLE`, `CH_DEDUP_BUF_TABLE`,
  список колонок — `MAIN_COLUMNS` (есть fallback на старое имя `HBASE_MAIN_COLUMNS`).
- **Каденс публикаций**: `PUBLISH_EVERY_MINUTES` **или** `PUBLISH_EVERY_SLICES`; финальная публикация — `ALWAYS_PUBLISH_AT_END=1`.
- Отдельный под‑журнал для дедупа: `process_name="<PROCESS_NAME>:dedup"` — каждая публикация атомарна и наблюдаема.

---

## Архитектура потока (коротко)
1) CLI → строим бизнес‑интервал `[--since, --until)`  
2) **PhoenixClient** нарезает/читает окно, возвращая батчи строк  
3) **CH‑клиент** вставляет в RAW (Distributed)  
4) По каденсу — **дедуп/публикация** в CLEAN через буфер `…_dedup_buf`  
5) **PG‑журнал** фиксирует статусы/метрики/heartbeat

---

## Подробно: как идёт поток данных
1. Интервал `[--since, --until)` режется на «слайсы» длиной `STEP_MIN` (по бизнес‑времени) и дополнительно
   **сдвигается/захлёстывается** для окна Phoenix:
   - `PHX_QUERY_SHIFT_MINUTES` — техсдвиг всего окна влево/вправо;
   - `PHX_QUERY_OVERLAP_MINUTES` — захлёст слева (по умолчанию только на первом слайсе);
   - `PHX_QUERY_LAG_MINUTES` — лаг справа, чтобы не брать «кипящее».
2. **PhoenixClient** использует начальную нарезку `PHX_INITIAL_SLICE_MIN` (по сетке UTC). Для каждого начального слайса:
   - выполняет запрос с ретраями и бэк‑оффом;
   - при перегрузке — рекурсивно делит окно до минимума `PHX_OVERLOAD_MIN_SPLIT_MIN` и глубины `PHX_OVERLOAD_MAX_DEPTH`.
3. Чтение batched (`PHX_FETCHMANY_SIZE`), локальный дедуп внутри запуска по ключу `(c, t, opd)`.
4. Приведение типов под DDL (`DateTime64(3)` как naive `datetime`, микросекунды → миллисекунды).
5. Пакетные **INSERT VALUES** в `CH_RAW_TABLE` (обычно `…_raw_all`) чанками `CH_INSERT_BATCH`.
6. Накапливаем затронутые партиции (`toYYYYMMDD(opd)`). По каденсу выполняется публикация:
   - `ALTER TABLE …_dedup_buf DROP PARTITION p` (очистка буфера),
   - `INSERT INTO …_dedup_buf SELECT c, t, opd, argMax(<прочие>, ingested_at)… FROM RAW_ALL WHERE part=p GROUP BY c,t,opd`,
   - `ALTER TABLE CLEAN REPLACE PARTITION p FROM …_dedup_buf`,
   - `ALTER TABLE …_dedup_buf DROP PARTITION p`.

---

## DDL (ClickHouse)
Все объекты — в `ddl/stg_daily_codes_history.sql`.  
Основные таблицы (локальные и Distributed):
- RAW локальная: `stg.daily_codes_history_raw`
- RAW Distributed (**источник дедупа**): `stg.daily_codes_history_raw_all`
- BUF (для дедупа, без TTL): `stg.daily_codes_history_dedup_buf`
- CLEAN локальная: `stg.daily_codes_history`
- CLEAN Distributed: `stg.daily_codes_history_all`

TTL у RAW по `ingested_at`. У CLEAN — бизнес‑TTL, пример в DDL:
```sql
TTL toDateTime(opd) + INTERVAL 5 DAY DELETE
```

---

## Требования
- Python 3.12+
- `clickhouse-driver` (с поддержкой zstd и cityhash), `phoenixdb`, `psycopg2-binary`/`psycopg[binary]`
- Для сжатия транспорта: `zstd`, `clickhouse-cityhash==1.0.2.4`

---

## Быстрый старт
```bash
# 1) Окружение
python3.12 -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -r requirements.txt

# 2) ClickHouse DDL
clickhouse-client -n --queries-file=ddl/stg_daily_codes_history.sql

# 3) ENV
cp .env.example .env   # отредактируйте доступы/хосты при необходимости

# 4) Запуск
python -m scripts.codes_history_etl \
  --since "2025-08-08T00:00:00Z" \
  --until "2025-08-09T00:00:00Z" \
  --log=INFO
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
PHX_TCP_PROBE_TIMEOUT_MS=1200        # TCP‑проба до первого запроса (не переименовывать!)

# --- Грануляция и PQS‑окна на уровне ETL ---
STEP_MIN=10                          # длина бизнес‑слайса
PHX_QUERY_SHIFT_MINUTES=-300         # техсдвиг всего окна (в минутах)
PHX_QUERY_OVERLAP_MINUTES=5          # захлёст слева
PHX_OVERLAP_ONLY_FIRST_SLICE=1       # 1 — только на первом слайсе; 0 — на всех
PHX_QUERY_LAG_MINUTES=2              # лаг справа (не брать «кипящее»)

# --- Адаптивная вытяжка (глубоко в PhoenixClient) ---
PHX_INITIAL_SLICE_MIN=5              # 0 — читать одним окном; >0 — начальная нарезка по сетке UTC
PHX_OVERLOAD_MIN_SPLIT_MIN=2         # минимальный размер рекурсивного сплита
PHX_OVERLOAD_MAX_DEPTH=3             # максимальная глубина рекурсивных делений
PHX_OVERLOAD_MAX_ATTEMPTS=4          # попытки на одно окно (с экспоненциальным бэк‑оффом)

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
Также логируется TZ‑контекст и фактическая нарезка слайсов: бизнес‑TZ, сдвиги и лаги.

---

## Журнал и надёжность
- **Эксклюзивность**: advisory‑lock по `process_name` + частичный UNIQUE‑индекс  
  `WHERE ts_end IS NULL AND status IN ('planned','running')`.
- **Автосанация** перед стартом: `sanitize_stale()`:
  - `planned` старше TTL → `skipped`;
  - `running` с протухшим heartbeat → `error`;
  - (опционально) `running` старше жёсткого TTL → `error`.
- **Под‑журнал публикации**: имя процесса `<PROCESS_NAME>:dedup`, интервалы строятся по множеству затронутых партиций.

---

## Тонкая настройка и производительность
- **Phoenix**: держите `PHX_FETCHMANY_SIZE` крупным (5–10k), следите за GC и сетевыми таймаутами;
  при перегрузке увеличивайте `PHX_INITIAL_SLICE_MIN` и/или позволяйте более глубокий сплит (`PHX_OVERLOAD_MAX_DEPTH`).
- **ClickHouse**: выбирайте RAW **Distributed** как цель insert; держите `CH_INSERT_BATCH` крупным (10–50k),
  `CH_INSERT_MAX_RETRIES=1` — достаточен (повтор при сетевой ошибке).
- **Дедуп**: ключ `(c,t,opd)`, по остальным колонкам — `argMax(col, ingested_at)`; проверяйте, что RAW_ALL — источник для дедупа.
- **TZ**: партиции в CH по `toYYYYMMDD(opd)` — это UTC; бизнес‑TZ отражается только в логике построения слайсов.

---

## Чек‑лист согласованности (ENV ↔ DDL ↔ код)
1. Таблицы в CH существуют и совпадают по именам:
   - `stg.daily_codes_history_raw`, `stg.daily_codes_history_raw_all`,
   - `stg.daily_codes_history_dedup_buf`,
   - `stg.daily_codes_history`, `stg.daily_codes_history_all`.
   Проверка:
   ```sql
   SHOW TABLES FROM stg LIKE 'daily_codes_history%';
   ```
2. В `.env` нет устаревших переменных (`CH_TABLE`, `HBASE_MAIN_COLUMNS`, `PHOENIX_TCP_PROBE_TIMEOUT_MS`).
3. В логах видно корректный TZ‑контекст и нарезку по `STEP_MIN`; стартовые слайсы ровно по сетке UTC при `PHX_INITIAL_SLICE_MIN>0`.
4. Ошибки CH по сети редки и гасятся на единственном повторе; при частых — проверьте сеть/кластер.
5. Список `MAIN_COLUMNS` полностью соответствует Phoenix‑схеме таблицы (без лишних пробелов/знаков).

---

## Частые проблемы и решения
- **`Undefined column ... TBL_JTI_TRACE_CIS_HISTORY.C`** — список `MAIN_COLUMNS` не совпадает со схемой Phoenix
  (проверьте имена и порядок, не оставляйте лишние запятые/пробелы).
- **`UniqueViolation inc_processing_active_one_uq_idx`** — уже есть активный запуск. Подождать завершения
  или перезапустить: перед стартом выполнится `sanitize_stale()`.
- **`can't subtract offset-naive and offset-aware datetimes`** — передавайте `--since/--until` оба либо с TZ, либо оба без TZ.
- **`TTL ... should have DateTime or Date, but has DateTime64`** — используйте `toDateTime(opd)` в TTL (см. DDL).
- **`No module named 'zstd'`** — `pip install zstd`.
- **`clickhouse_driver.errors.UnexpectedPacketFromServerError`** — клиент сделает reconnect+повтор; если часто — сеть.

---

## Структура проекта
- `scripts/codes_history_etl.py` — основной ETL и дедуп/публикация.
- `scripts/journal.py` — журнал в PG / watermark / эксклюзив.
- `scripts/db/phoenix_client.py` — клиент PQS (адаптивная вытяжка, fetchmany).
- `scripts/db/clickhouse_client.py` — ClickHouse native (failover, retries, reconnect).
- `scripts/config.py` — загрузка ENV, дефолты, совместимость имён переменных.
- `ddl/stg_daily_codes_history.sql` — все таблицы RAW/CLEAN/BUF (+ Distributed).

---

## Лицензия / Авторство
Внутренний служебный проект.