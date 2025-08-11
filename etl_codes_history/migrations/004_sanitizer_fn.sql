-- 004_sanitizer_fn.sql
-- Purpose: Provide a maintenance function to clean up stale 'planned' and 'running' rows.
-- Strategy:
--   * planned older than planned_ttl are marked 'skipped' with reason
--   * running with stale heartbeat (now - hb_at > running_ttl) are marked 'error' with reason
--   * returns counts for each category
-- NOTE: You can schedule this via pg_cron (optional).

CREATE OR REPLACE FUNCTION public.inc_processing_sanitize(
    planned_ttl  INTERVAL DEFAULT INTERVAL '10 minutes',
    running_ttl  INTERVAL DEFAULT INTERVAL '30 minutes',
    process_name TEXT     DEFAULT NULL  -- NULL => all processes
)
RETURNS TABLE(cleaned_planned BIGINT, cleaned_running BIGINT) LANGUAGE plpgsql AS
$$
DECLARE
    v_cleaned_planned BIGINT;
    v_cleaned_running BIGINT;
BEGIN
    -- 1) planned -> skipped by TTL
    WITH upd AS (
        UPDATE public.inc_processing t
           SET status = 'skipped',
               ts_end = now(),
               details = COALESCE(t.details, '{}'::jsonb) ||
                         jsonb_build_object('sanitized_reason','planned_ttl_expired',
                                            'sanitized_at', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SSOF'))
         WHERE t.ts_end IS NULL
           AND t.status = 'planned'
           AND now() - t.ts_start > planned_ttl
           AND (process_name = inc_processing_sanitize.process_name OR inc_processing_sanitize.process_name IS NULL)
        RETURNING 1
    )
    SELECT count(*) INTO v_cleaned_planned FROM upd;

    -- 2) running -> error by stale heartbeat
    WITH upd AS (
        UPDATE public.inc_processing t
           SET status = 'error',
               ts_end = now(),
               details = COALESCE(t.details, '{}'::jsonb) ||
                         jsonb_build_object('sanitized_reason','running_heartbeat_stale',
                                            'sanitized_at', to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SSOF'))
         WHERE t.ts_end IS NULL
           AND t.status = 'running'
           AND (now() - COALESCE(t.hb_at, t.ts_start)) > running_ttl
           AND (process_name = inc_processing_sanitize.process_name OR inc_processing_sanitize.process_name IS NULL)
        RETURNING 1
    )
    SELECT count(*) INTO v_cleaned_running FROM upd;

    RETURN QUERY SELECT v_cleaned_planned, v_cleaned_running;
END;
$$;
