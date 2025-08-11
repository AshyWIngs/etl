-- 001_bootstrap_journal.sql
-- Создание таблиц журнала и состояния + базовые индексы/чек-констрейнт.
BEGIN;
CREATE TABLE IF NOT EXISTS public.inc_processing (
    id            BIGSERIAL PRIMARY KEY,
    process_name  TEXT        NOT NULL,
    ts_start      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ts_end        TIMESTAMPTZ NULL,
    status        TEXT        NOT NULL,
    details       JSONB       NULL,
    host          TEXT        NULL,
    pid           INTEGER     NULL
);
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'inc_processing'
      AND c.conname = 'inc_processing_status_chk'
  ) THEN
    EXECUTE 'ALTER TABLE public.inc_processing DROP CONSTRAINT inc_processing_status_chk';
  END IF;
END$$;
ALTER TABLE public.inc_processing
  ADD CONSTRAINT inc_processing_status_chk
  CHECK (status IN ('planned','running','ok','error','skipped'));
CREATE INDEX IF NOT EXISTS inc_processing_pname_started_desc_idx
  ON public.inc_processing (process_name, ts_start DESC);
CREATE INDEX IF NOT EXISTS inc_processing_active_only_idx
  ON public.inc_processing (process_name)
  WHERE ts_end IS NULL AND status IN ('planned','running');
CREATE INDEX IF NOT EXISTS inc_processing_ended_notnull_idx
  ON public.inc_processing (ts_end)
  WHERE ts_end IS NOT NULL;
CREATE TABLE IF NOT EXISTS public.inc_process_state (
    process_name TEXT PRIMARY KEY,
    watermark    TIMESTAMPTZ NULL,
    origin       TEXT        NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS inc_process_state_updated_at_idx
  ON public.inc_process_state (updated_at DESC);
CREATE INDEX IF NOT EXISTS inc_process_state_watermark_idx
  ON public.inc_process_state (watermark);
COMMIT;
