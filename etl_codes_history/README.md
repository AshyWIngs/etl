# ETL: Phoenix/HBase → ClickHouse (with PG journal)

Лёгкий инкрементальный перенос из Phoenix (HBase) в ClickHouse **напрямую** (без CSV),
с журналом запусков и `watermark` в PostgreSQL. Работает через **native 9000** порт
ClickHouse с включённой компрессией.

---
## Что делает
- Читает окно по времени из таблицы-источника Phoenix (`TBL_JTI_TRACE_CIS_HISTORY`).
- Пакетно выгружает строки (fetchmany) и сразу пишет в ClickHouse **native** (со сжатием).
- В PostgreSQL ведёт журнал запусков (planned → running → success/error/skip) и хранит watermark.
- Защита от параллельных запусков: advisory‑lock + частичный UNIQUE‑индекс (один активный запуск).
- Автосанация «висячих» запусков перед стартом.
- **Пост‑обработка партиций в CH:** дедупликация по ключу `(c, t, opd)` и публикация партиций
  в чистую таблицу через `REPLACE PARTITION` (см. ниже).
- **Надёжность CH‑вставок и DDL:** авто‑переподключение и один повтор при «плавающих» сетевых
  сбоях драйвера (`UnexpectedPacketFromServerError`).
- **Метрики дедупа:** логируются и пишутся в журнал как heartbeat (этап/партиция/время).

---
## Новое (ключевые изменения)
- Клиент ClickHouse (native 9000) с **failover по списку хостов** и health‑check `SELECT 1`.
- Вставка в RAW таблицу **чанками** (`CH_INSERT_BATCH`) с **повтором** при ошибке соединения
  (`CH_INSERT_MAX_RETRIES`, по умолчанию 1) и принудительным `reconnect()` при необходимости.
- Перед тяжёлыми `INSERT SELECT`/`ALTER` в дедупе — **принудительный reconnect** для сброса
  возможного «подвешенного» состояния сокета после bulk‑вставок.
- Локальная обёртка для DDL/`INSERT SELECT` с **одним повтором** при
  `UnexpectedPacketFromServerError` (включая кейс *EndOfStream*).
- **Метрики времени** по шагам дедупа: `DROP BUF`, `INSERT BUF`, `REPLACE CLEAN`, `CLEANUP BUF`,
  а также суммарно по партиции.

---
## Как это работает (коротко)
1. Итерация по бизнес‑окну времени с шагом `STEP_MIN`; окно запроса в Phoenix
   сдвигается на `PHX_QUERY_SHIFT_MINUTES` (например, −300 мин).
2. Чтение из Phoenix батчами; в процессе ETL выполняется **локальный дедуп** в рамках запуска
   по ключу `(c, t, opd)` (чтобы не гнать дубли в RAW).
3. Приведение типов под DDL CH (в т.ч. `DateTime64(3)` как `datetime` без TZ, миллисекунды округляются вниз).
4. Пакетные INSERT'ы сразу в **RAW_ALL** (Distributed) таблицу ClickHouse.
5. После загрузки всего интервала вычисляются затронутые **партиции** (по `toYYYYMMDD(opd)`):
   - `DROP PARTITION` в буфере (очистка),
   - `INSERT INTO BUF SELECT ... FROM RAW_ALL` с агрегацией `argMax(col, ingested_at)`
     по всем неключевым колонкам — тем самым выбирается **самая свежая** версия строки,
   - `ALTER ... REPLACE PARTITION` из BUF → в CLEAN (локальная/мерджируемая),
   - финальный `DROP PARTITION` в BUF.
6. Каждый шаг сопровождается хартбитом и логами с длительностью по партиции/этапу.

---
## Требования
- **Python 3.12** (рекомендуется).
- Зависимости для **native‑сжатия**: `lz4`, `zstd`, **`clickhouse-cityhash`** (обязательно для сжатия).
- Доступ к Phoenix PQS и ClickHouse (native 9000), PostgreSQL для журнала.

> Примечание: Python 3.13 может работать, но готовые колёса `clickhouse-cityhash` часто недоступны — используйте 3.12.

---
## Быстрый старт
```bash
# 1) Виртуальное окружение (Python 3.12)
python3.12 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# 2) ClickHouse DDL (локальная таблица + Distributed)
clickhouse-client -n --queries-file=ddl/stg_daily_codes_history.sql

# 3) Настройки окружения
cp .env.example .env   # заполните значения

# 4) Запуск инкремента
python -m scripts.codes_history_etl \
  --since "2025-08-08T00:00:00Z" \
  --until "2025-08-09T00:00:00Z"
```
PG‑таблицы журнала создаются автоматически при первом запуске (см. `journal.ensure()`).

---
## ENV (минимум)
Создайте `.env` на основе `.env.example`. Ключевые переменные:
```ini
# file: .env.example

# --- PostgreSQL (журнал) ---
PG_DSN=postgresql://etl_user:password@10.254.3.91:5432/etl_database?connect_timeout=5&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5
JOURNAL_TABLE=public.inc_processing
PROCESS_NAME=codes_history_increment
JOURNAL_RETENTION_DAYS=1         # опционально: для функции очистки журнала (ProcessJournal.prune_old)

# --- Phoenix ---
PQS_URL=http://10.254.3.112:8765
PHX_FETCHMANY_SIZE=5000
PHX_TS_UNITS=timestamp             # 'seconds' | 'millis' | 'timestamp' — для phoenixdb
PHX_QUERY_SHIFT_MINUTES=-300       # сдвиг окна запроса (минуты) относительно бизнес-интервала
HBASE_MAIN_TABLE=TBL_JTI_TRACE_CIS_HISTORY
HBASE_MAIN_TS_COLUMN=opd
HBASE_MAIN_COLUMNS=c,t,opd,id,did,rid,rinn,rn,sid,sinn,sn,gt,prid,st,ste,elr,emd,apd,exd,p,pt,o,pn,b,tt,tm,ch,j,pg,et,pvad,ag

# --- ClickHouse (только native:9000) ---
CH_HOSTS=10.254.3.111,10.254.3.112,10.254.3.113,10.254.3.114
CH_PORT=9000
CH_DB=stg
CH_USER=default
CH_PASSWORD=
# Имена таблиц (см. DDL):
CH_RAW_TABLE=stg.daily_codes_history_raw_all
CH_CLEAN_TABLE=stg.daily_codes_history
CH_CLEAN_ALL_TABLE=stg.daily_codes_history_all
CH_DEDUP_BUF_TABLE=stg.daily_codes_history_dedup_buf
# Параметры вставки
CH_INSERT_BATCH=20000              # размер чанка при INSERT VALUES
CH_INSERT_MAX_RETRIES=1            # число повторов на чанк при сетевой ошибке

# --- Параметры ETL ---
STEP_MIN=60                        # размер слайда (минуты)
```

---
## Тонкая настройка и производительность
- **Phoenix**
  - `PHX_FETCHMANY_SIZE` — размер пакета для курсора; подбирайте по памяти сети/кластера.
  - Сдвиг запроса `PHX_QUERY_SHIFT_MINUTES` (обычно −300) компенсирует временную зону/задержки источника.
- **ClickHouse**
  - Клиент использует сжатие транспорта (по умолчанию ZSTD). Требуются `lz4`, `zstd`, `clickhouse-cityhash`.
  - Асинхронные вставки включены: `async_insert=1`, `wait_for_async_insert=1`,
    для Distributed — `insert_distributed_sync=1`.
  - Вставки идут **чанками** `CH_INSERT_BATCH`; при ошибках выполняется **failover + reconnect** и повтор.
- **Дедуп/публикация**
  - Агрегация: `argMax(col, ingested_at)` для всех неключевых колонок; ключ группировки — `(c, t, opd)`.
  - Партиции определяются по диапазону фактического запроса (после сдвига) как `toYYYYMMDD(opd)`.

---
## Наблюдаемость (метрики дедупа)
В лог и в `inc_processing.details` (через heartbeat) попадают записи вида:
```json
{"stage":"insert dedup part 20250812","part":20250812,"ms":12345}
{"stage":"replace clean part 20250812","part":20250812,"ms":2345}
{"stage":"cleanup buf partition 20250812","part":20250812,"ms":321}
{"stage":"dedup_part_total_ms","part":20250812,"ms":15011}
```
Это помогает быстро локализовать «узкие места» дедупа.

---
## Политика данных в CH
- Переносим **как есть**: значения колонок совпадают с источником (без искусственных заполнений).
- Уникальность для дедупа: комбинация `c, t, opd`.
- Публикация в CLEAN идемпотентна: собираем партицию в BUF → `REPLACE PARTITION` в CLEAN → чистим BUF.

---
## Частые проблемы и решения
- **`UniqueViolation inc_processing_active_one_uq_idx`** — уже есть активный запуск.
  - Либо дождитесь завершения, либо выполните санацию: код делает `sanitize_stale()` перед стартом.
- **`can't subtract offset-naive and offset-aware datetimes`** — передайте `--since/--until` с таймзоной
  (например, `...T00:00:00Z` или `...T00:00:00+05:00`).
- **`TTL ... should have DateTime or Date, but has DateTime64`** — используйте в TTL `toDateTime(...)`
  или поля без суффикса `(3)` (см. актуальный DDL в `ddl/`).
- **`No module named 'zstd'`** — установите C‑расширение Zstandard:
  ```bash
  pip install zstd
  ```
- **`Package clickhouse-cityhash is required to use compression`** — установите:
  ```bash
  pip install "clickhouse-cityhash==1.0.2.4"
  ```
  Временный обходной путь — запустить клиент без сжатия (`compression=False`), но это увеличит трафик.
- **`clickhouse_driver.errors.UnexpectedPacketFromServerError` (Code 102, EndOfStream и т.п.)** —
  встроенный обёрткой выполняется `reconnect()` и **один повтор** операции. Обычно этого достаточно.

---
## Структура проекта
- `scripts/codes_history_etl.py` — основной ETL, дедуп/публикация партиций, метрики.
- `scripts/journal.py` — журнал в PG, watermark, эксклюзивная блокировка, санация stale.
- `scripts/db/phoenix_client.py` — клиент Phoenix PQS (fetch_increment).
- `scripts/db/pg_client.py` — тонкая обёртка над psycopg.
- `scripts/db/clickhouse_client.py` — ClickHouse native (9000, compression, zstd/lz4, failover, retries).
- `scripts/config.py` — загрузка ENV/валидация.
- `ddl/stg_daily_codes_history.sql` — DDL ClickHouse (локальная и Distributed таблицы, RAW/CLEAN/BUF).

---
## Локальный гайд по Git
- В репозитории игнорируется `.env`, но **`.env.example` хранится**.
- Рекомендуемая команда для обхода глобальных игноров при первом добавлении шаблона:
  ```bash
  git add -f etl_codes_history/.env.example && git commit -m "Add .env.example"
  ```

---
## Лицензия / Авторство
Внутренний служебный проект.