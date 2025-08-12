-- =====================================================================
-- 02_upsert_window.sql  (опционально)
-- =====================================================================
-- Сценарий на случай ручного/разового бэктилла (если MV временно отключен)
-- или если нужно переиграть конкретное окно:
--   1) Собираем по окну [since, until) уникальные (c,t,opd) из сырца.
--   2) Вставляем в финал только те ключи, которых ещё нет.
-- Никаких удалений в финале не делаем — это безопасный "idempotent insert".
-- =====================================================================

-- Задайте окно
-- Например:
--   SET since = toDateTime64('2025-08-08 00:00:00', 3, 'UTC');
--   SET until = toDateTime64('2025-08-09 00:00:00', 3, 'UTC');

WITH
    toDateTime64({since:String}, 3, 'UTC') AS since_,
    toDateTime64({until:String}, 3, 'UTC') AS until_
INSERT INTO stg.daily_codes_history
SELECT
    g.c, g.t, g.opd,
    g.did, g.rid, g.rinn, g.rn, g.sid, g.sinn, g.sn, g.gt, g.prid,
    g.st, g.ste, g.elr, g.emd, g.apd, g.p, g.pt, g.o, g.pn, g.b,
    g.tt, g.tm, g.ch, g.j, g.pg, g.et, g.exd, g.pvad, g.ag,
    now64(3) AS ingested_at
FROM
(
    SELECT
        c, t, opd,
        any(did)  AS did,
        any(rid)  AS rid,
        any(rinn) AS rinn,
        any(rn)   AS rn,
        any(sid)  AS sid,
        any(sinn) AS sinn,
        any(sn)   AS sn,
        any(gt)   AS gt,
        any(prid) AS prid,
        any(st)   AS st,
        any(ste)  AS ste,
        any(elr)  AS elr,
        any(emd)  AS emd,
        any(apd)  AS apd,
        any(p)    AS p,
        any(pt)   AS pt,
        any(o)    AS o,
        any(pn)   AS pn,
        any(b)    AS b,
        any(tt)   AS tt,
        any(tm)   AS tm,
        arraySort(arrayDistinct(arrayFlatten(groupArray(ch)))) AS ch,
        any(j)    AS j,
        any(pg)   AS pg,
        any(et)   AS et,
        any(exd)  AS exd,
        any(pvad) AS pvad,
        any(ag)   AS ag
    FROM stg.daily_codes_history_all
    WHERE opd >= since_ AND opd < until_
    GROUP BY c, t, opd
) AS g
LEFT JOIN stg.daily_codes_history AS f USING (c, t, opd)
WHERE f.c IS NULL
;