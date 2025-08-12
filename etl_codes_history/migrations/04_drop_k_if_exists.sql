-- =====================================================================
-- 04_drop_k_if_exists.sql — чистим наследие (если вдруг осталась колонка k)
-- =====================================================================
ALTER TABLE stg.daily_codes_history_all DROP COLUMN IF EXISTS k;
ALTER TABLE stg.daily_codes_history      DROP COLUMN IF EXISTS k;