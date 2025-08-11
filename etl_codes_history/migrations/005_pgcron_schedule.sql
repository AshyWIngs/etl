-- 005_pgcron_schedule.sql (optional)
-- If pg_cron is installed in your cluster, this will schedule the sanitizer every 5 minutes.
-- Otherwise, this script is a no-op (errors are suppressed).
DO
$$
BEGIN
    -- Try to create extension
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_cron;
    EXCEPTION WHEN OTHERS THEN
        -- Ignore if not available
        NULL;
    END;

    -- Schedule job only if pg_cron exists
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule(
            job_name := 'inc_processing_sanitize_every_5min',
            schedule := '*/5 * * * *',
            command  := $$SELECT public.inc_processing_sanitize('10 minutes','30 minutes', NULL);$$
        );
    END IF;
END
$$;
