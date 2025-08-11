-- 003_add_heartbeat_and_window_idx.sql
-- Purpose:
--   * Add heartbeat column (hb_at) to inc_processing for liveness checks
--   * Add expression UNIQUE index for active window to prevent duplicate planned/running
--   * Keep existing indexes intact (no-op if they already exist)
-- Safe to run repeatedly.

BEGIN;

-- 1) hb_at column
ALTER TABLE public.inc_processing
    ADD COLUMN IF NOT EXISTS hb_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Optional backfill: for very old rows hb_at may predate ts_end; leave as-is

-- 2) Expression UNIQUE index for active window (process_name + slice_from + slice_to)
--    details->>'slice_from' / 'slice_to' are ISO8601 text values
CREATE UNIQUE INDEX IF NOT EXISTS inc_processing_active_window_uniq
  ON public.inc_processing ((details->>'slice_from'), (details->>'slice_to'), process_name)
  WHERE ts_end IS NULL AND status IN ('planned','running');

COMMIT;
