# ETL: Phoenix/HBase → ClickHouse (with PG journal)

Лёгкий инкрементальный перенос из Phoenix (HBase) в ClickHouse **напрямую** (без CSV),
с журналом запусков и `watermark` в PostgreSQL. Работает через **native 9000** порт
ClickHouse с включённой компрессией.

---
## Что делает
- Читает окно по времени из таблицы-источника Phoenix (`TBL_JTI_TRACE_CIS_HISTORY`).
- Пакетно выгружает строки (fetchmany) и сразу пишет в CH (native, compression).
- В PostgreSQL ведёт журнал запусков (planned → running → success/error/skip) и хранит watermark.
- Защита от параллельных запусков: advisory-lock + частичный UNIQUE-индекс (один активный запуск).
- Автосанация «висячих» запусков перед стартом.

---
## Требования
- **Python 3.12** (рекомендуется; для режима сжатия нужен wheel `clickhouse-cityhash`).
- Зависимости для компрессии native‑клиента: `lz4`, `zstd`, `clickhouse-cityhash` (см. `requirements.txt`).
- Доступ к Phoenix PQS и к ClickHouse (native 9000).
- PostgreSQL для журнала.

> Примечание: Python 3.13 может работать, но готовые колёса `clickhouse-cityhash` бывают недоступны; остаёмся на 3.12.

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
CH_TABLE=stg.daily_codes_history_all
CH_INSERT_BATCH=20000
CH_INSERT_MAX_RETRIES=1

# --- Параметры ETL ---
STEP_MIN=60        # размер слайда (минуты)
```

---
## Структура проекта
- `scripts/codes_history_etl.py` — основной ETL.
- `scripts/journal.py` — журнал в PG, watermark, эксклюзивная блокировка, санация stale.
- `scripts/db/phoenix_client.py` — клиент Phoenix PQS (fetch_increment).
- `scripts/db/pg_client.py` — тонкая обёртка над psycopg.
- `scripts/db/clickhouse_client.py` — ClickHouse native (9000, compression, zstd/lz4).
- `scripts/config.py` — загрузка ENV/валидация.
- `ddl/stg_daily_codes_history.sql` — DDL ClickHouse (локальная и Distributed таблицы).

---
## Политика данных в CH
- Переносим **как есть**: значения колонок совпадают с источником (без искусственных заполнений).
- Уникальность на чтении/обработке определяется комбинацией `c, t, opd` (для последующей дедупликации).
- (Опционально в DDL) TTL для авто-удаления старых записей (пример в DDL).

---
## Частые проблемы и решения
- **`UniqueViolation inc_processing_active_one_uq_idx`** — уже есть активный запуск.
  - Либо дождитесь завершения, либо выполните санацию: код делает `sanitize_stale()` перед стартом.
- **`can't subtract offset-naive and offset-aware datetimes`** — передайте `--since/--until` с таймзоной
  (например, `...T00:00:00Z` или `...T00:00:00+05:00`).
- **`TTL ... should have DateTime or Date, but has DateTime64`** — используйте в TTL `toDateTime(...)` или поля без суффикса `(3)` (см. актуальный DDL в `ddl/`).
- **`No module named 'zstd'`** — установите C‑расширение Zstandard:
  ```bash
  pip install zstd
  ```
- **`Package clickhouse-cityhash is required to use compression`** — установите:
  ```bash
  pip install "clickhouse-cityhash==1.0.2.4"
  ```
  Если временно не можете поставить — запустите без сжатия, передав `compression=False` при создании `CHClient` (будет больше трафика).

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