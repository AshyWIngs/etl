# Миграции для журнала запусков ETL

В репозитории предусмотрены миграции для PostgreSQL, которые можно применять вручную,
а также есть встроенная инициализация в коде (`ProcessJournal.ensure()`) и встроенные
SQL-миграции (`ProcessJournal.apply_sql_migrations()`), делающие то же самое идемпотентно.

## Файлы

1. `migrations/001_bootstrap_journal.sql` — создаёт таблицы `public.inc_processing`
   и `public.inc_process_state`, базовые индексы и чек‑констрейнт статуса.
2. `migrations/002_one_active_guard.sql` — создаёт частичный **уникальный** индекс
   `inc_processing_one_active_unique_idx`, запрещающий иметь более одной активной
   записи (`planned`/`running` без `ts_end`) на процесс.

## Как применять вручную

```bash
psql "$PG_DSN" -f migrations/001_bootstrap_journal.sql
psql "$PG_DSN" -f migrations/002_one_active_guard.sql
```

## Как применить из кода

*Автоматически при старте*: вызывается `journal.ensure()` в `codes_history_etl.py`.

*Явно, отдельным шагом*:
```bash
python -m scripts.codes_history_etl --migrate-only --until "1970-01-01T00:00:00Z"
```
(параметр `--until` обязателен у argparse, но в режиме `--migrate-only` он не используется).

## Рекомендация

Оставить оба подхода. В DEV/standalone — полагаться на `ensure()`,
в PROD — прогонять SQL‑миграции (или `--migrate-only`) и всё равно держать `ensure()`
на случай инцидентов.
