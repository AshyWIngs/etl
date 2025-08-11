# Journal migrations — heartbeat & sanitizer

This folder adds two optional but recommended migrations on top of the bootstrap:

* **003_add_heartbeat_and_window_idx.sql**
  * Adds `hb_at TIMESTAMPTZ NOT NULL DEFAULT now()` — heartbeat timestamp
  * Adds unique index `inc_processing_active_window_uniq` to prevent concurrent active duplicates for the same window `(slice_from, slice_to, process_name)`
* **004_sanitizer_fn.sql**
  * Adds `inc_processing_sanitize(planned_ttl, running_ttl, process_name)`
  * Marks long-stuck `planned` as `skipped`, and long-stuck `running` (no heartbeat) as `error`
* **005_pgcron_schedule.sql** (optional)
  * Schedules the sanitizer to run every 5 minutes (requires `pg_cron`)

## Apply

```bash
psql "$PG_DSN" -f migrations/003_add_heartbeat_and_window_idx.sql
psql "$PG_DSN" -f migrations/004_sanitizer_fn.sql
# optional
psql "$PG_DSN" -f migrations/005_pgcron_schedule.sql
```

Or via the Python entry point (if supported): `python -m scripts.codes_history_etl --migrate-only --until "1970-01-01T00:00:00Z"`

## How the sanitizer decides
* An instance is **alive** if it keeps updating `hb_at`. Your worker should ping heartbeat periodically (e.g. every 15–30s).
* `planned` older than `planned_ttl` → `skipped` with reason `planned_ttl_expired`.
* `running` with `now() - hb_at > running_ttl` → `error` with reason `running_heartbeat_stale`.

Tune TTLs per environment. For long Phoenix scans use larger `running_ttl`.

