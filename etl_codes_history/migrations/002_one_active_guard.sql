-- 002_one_active_guard.sql
-- Гарантирует, что одновременно у процесса не более одной активной записи
-- ('planned' или 'running' с ts_end IS NULL).
BEGIN;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname  = 'inc_processing_one_active_unique_idx'
  ) THEN
    EXECUTE '
      CREATE UNIQUE INDEX inc_processing_one_active_unique_idx
        ON public.inc_processing (process_name)
        WHERE ts_end IS NULL AND status IN (''planned'',''running'');';
  END IF;
END$$;
COMMIT;
