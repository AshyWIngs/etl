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
- **Python 3.12** (нужен для `clickhouse-cityhash`).
- Доступ к Phoenix PQS и к ClickHouse (native 9000).
- PostgreSQL для журнала.

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

# 4) (Опционально) прогнать миграции/инициализацию PG из кода
python -m scripts.codes_history_etl --migrate-only

# 5) Запуск инкремента
python -m scripts.codes_history_etl \
  --since "2025-08-08T00:00:00Z" \
  --until "2025-08-09T00:00:00Z"
```
**Примечание про время:** передавайте явный часовой пояс (`Z` или `+05:00`).

---
## ENV (минимум)
Создайте `.env` на основе `.env.example`. Ключевые переменные:
```ini
# PostgreSQL (журнал и watermark)
PG_DSN=postgresql://user:pass@host:5432/dbname
JOURNAL_TABLE=public.inc_processing
PROCESS_NAME=codes_history_increment

# Phoenix PQS
PQS_URL=http://10.254.3.112:8765
PHX_FETCHMANY_SIZE=5000
PHX_TS_UNITS=timestamp   # timestamp | millis

# Источник (Phoenix/HBase)
HBASE_MAIN_TABLE=TBL_JTI_TRACE_CIS_HISTORY
HBASE_MAIN_TS_COLUMN=opd

# ClickHouse (native 9000 + компрессия)
CLICKHOUSE_HOST=10.254.3.114
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=stg
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_COMPRESSION=1  # включить сжатие в драйвере

# Логика окна (по умолчанию)
STEP_MIN=60
BUSINESS_TZ=Asia/Almaty
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