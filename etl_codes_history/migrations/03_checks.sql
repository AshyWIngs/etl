-- =====================================================================
-- 03_checks.sql — быстрые проверки качества
-- =====================================================================

-- 1) Проверить, что в финале нет дублей по (c,t,opd)
SELECT c, t, opd, count() AS cnt
FROM stg.daily_codes_history
GROUP BY c, t, opd
HAVING cnt > 1
ORDER BY cnt DESC, opd DESC
LIMIT 100;

-- 2) Посмотреть возможные дубли в сырце за окно (пример)
--    Параметризуйте since/until нужными датами
WITH
    toDateTime64({since:String}, 3, 'UTC') AS since_,
    toDateTime64({until:String}, 3, 'UTC') AS until_
SELECT c, t, opd, count() AS cnt
FROM stg.daily_codes_history_all
WHERE opd >= since_ AND opd < until_
GROUP BY c, t, opd
HAVING cnt > 1
ORDER BY cnt DESC, opd DESC
LIMIT 100;

-- 3) Сверка объёмов: сколько уникальных ключей в сырце vs сколько строк в финале
WITH
    toDateTime64({since:String}, 3, 'UTC') AS since_,
    toDateTime64({until:String}, 3, 'UTC') AS until_
SELECT
    'raw_unique' AS src,
    countDistinct((c, t, opd)) AS rows_
FROM stg.daily_codes_history_all
WHERE opd >= since_ AND opd < until_
UNION ALL
SELECT
    'final_rows' AS src,
    count() AS rows_
FROM stg.daily_codes_history
WHERE opd >= since_ AND opd < until_;