-- =====================================================================
-- 01_clickhouse_ddl.sql
-- =====================================================================
-- Цель: Полностью автоматизировать дедупликацию по (c, t, opd) на стороне ClickHouse
--       без участия человека. ETL пишет ТОЛЬКО в сырцовую таблицу (_all),
--       а MATERIALIZED VIEW сам раскладывает данные в итоговую таблицу
--       stg.daily_codes_history, гарантируя, что каждая тройка (c,t,opd)
--       появится максимум один раз.
--
-- Ключевые идеи:
--   1) Уникальность = ровно по (c, t, opd). Никаких хэшей.
--   2) В финальную таблицу данные попадают из сырой через MATERIALIZED VIEW:
--      - внутри одного батча (блока вставки) объединяем возможные дубли
--        с одинаковыми (c,t,opd) => GROUP BY ... + any()/arrayDistinct()
--      - исключаем ключи, которые уже есть в финальной таблице => LEFT JOIN ... WHERE f.c IS NULL
--   3) В результате: ни в таблице, ни в выдаче SELECT нет дублей по (c,t,opd).
--   4) Все таймстемпы храним "как в источнике" (opd — DateTime64(3)).
--      Смещение -5 часов используется только в источнике (в вашем коде запроса к Phoenix),
--      в ClickHouse ничего не сдвигаем.
--
-- Важно: используем именно ReplicatedMergeTree с путями, которые вы попросили,
-- чтобы шард/реплика реплицировались корректно.
-- =====================================================================

DROP VIEW IF EXISTS stg.mv_daily_codes_history_dedup SYNC;
DROP TABLE IF EXISTS stg.daily_codes_history SYNC;
DROP TABLE IF EXISTS stg.daily_codes_history_all SYNC;

-- ---------------------------------------------------------------------
-- СЫРЬЕВАЯ ТАБЛИЦА: получает всё, что приходит из Phoenix/CSV без логики.
-- Здесь допускаются повторы строк с одинаковыми (c,t,opd).
-- ---------------------------------------------------------------------
CREATE TABLE stg.daily_codes_history_all
(
    -- Ключ уникальности (требование):
    c  String                                    COMMENT 'cis: Код маркировки (ключ 1/3)',
    t  Int32                                     COMMENT 'operation type: Тип операции (ключ 2/3)',
    opd DateTime64(3, \'UTC\')                   COMMENT 'operation_date: Дата/время операции (ключ 3/3)',

    -- Остальные поля источника (делаем Nullable/строки, чтобы не падать на NULL/типах):
    did  Nullable(String),
    rid  Nullable(String),
    rinn Nullable(String),
    rn   Nullable(String),
    sid  Nullable(String),
    sinn Nullable(String),
    sn   Nullable(String),
    gt   Nullable(String),
    prid Nullable(String),
    st   Nullable(Int32),
    ste  Nullable(Int32),
    elr  Nullable(Int32),
    emd  Nullable(Int32),
    apd  Nullable(Int32),
    p    Nullable(String),
    pt   Nullable(Int32),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),
    tt   Nullable(Int32),
    tm   Nullable(Int32),

    -- Детские (дочерние) коды как массив строк (как вы просили):
    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),

    j    Nullable(String),
    pg   Nullable(Int32),
    et   Nullable(Int32),
    exd  Nullable(Int32),
    pvad Nullable(String),
    ag   Nullable(String),

    -- Системное поле для трассировки загрузки (не участвует в уникальности):
    ts   Nullable(DateTime64(3, 'UTC')) COMMENT 'время приема строки в сырец (из ETL)'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history_all', '{shardless_repl}')
PARTITION BY toDate(opd)
ORDER BY (opd, c, t)  -- оптимизируем под диапазоны по opd и быструю сортировку по ключам
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- ФИНАЛЬНАЯ ТАБЛИЦА: здесь каждая (c,t,opd) встречается максимум один раз.
-- Вставка происходит только из MATERIALIZED VIEW ниже.
-- ---------------------------------------------------------------------
CREATE TABLE stg.daily_codes_history
(
    c   String,
    t   Int32,
    opd DateTime64(3, 'UTC'),

    did  Nullable(String),
    rid  Nullable(String),
    rinn Nullable(String),
    rn   Nullable(String),
    sid  Nullable(String),
    sinn Nullable(String),
    sn   Nullable(String),
    gt   Nullable(String),
    prid Nullable(String),
    st   Nullable(Int32),
    ste  Nullable(Int32),
    elr  Nullable(Int32),
    emd  Nullable(Int32),
    apd  Nullable(Int32),
    p    Nullable(String),
    pt   Nullable(Int32),
    o    Nullable(String),
    pn   Nullable(String),
    b    Nullable(String),
    tt   Nullable(Int32),
    tm   Nullable(Int32),
    ch   Array(String) DEFAULT [] CODEC(ZSTD(6)),
    j    Nullable(String),
    pg   Nullable(Int32),
    et   Nullable(Int32),
    exd  Nullable(Int32),
    pvad Nullable(String),
    ag   Nullable(String),

    -- Когда строка фактически долетела до финальной таблицы (для аудита):
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/stg.daily_codes_history', '{shardless_repl}')
PARTITION BY toDate(opd)
ORDER BY (c, t, opd)  -- ключ сортировки строго по требуемой уникальности
SETTINGS index_granularity = 8192;

-- Индекс-ускоритель по (opd) для типичных диапазонных запросов
-- (опционально; можно удалить, если не нужен)
-- CREATE INDEX idx_opd_minmax ON stg.daily_codes_history (opd) TYPE minmax GRANULARITY 1;

-- ---------------------------------------------------------------------
-- MATERIALIZED VIEW: полностью автоматический дедуплятор на вставке в сырец.
-- Логика:
--   1) Внутри одного вставленного блока (батча) объединяем дубли по (c,t,opd):
--        GROUP BY c,t,opd + any(...) для остальных полей
--        Для ch объединяем и нормализуем: groupArray(ch) -> arrayFlatten -> arrayDistinct -> arraySort
--   2) Исключаем ключи, которые уже есть в финальной таблице:
--        LEFT JOIN stg.daily_codes_history AS f USING (c,t,opd) WHERE f.c IS NULL
--      Это предотвращает повторную вставку уже присутствующих ключей при повторном прогоне ETL.
--
-- ВАЖНО: MV видит ТОЛЬКО свежевставленный батч в stg.daily_codes_history_all,
-- поэтому GROUP BY агрегирует только новые строки, а не всю таблицу.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW stg.mv_daily_codes_history_dedup
TO stg.daily_codes_history
AS
WITH now64(3) AS _now
SELECT
    g.c, g.t, g.opd,
    g.did, g.rid, g.rinn, g.rn, g.sid, g.sinn, g.sn, g.gt, g.prid,
    g.st, g.ste, g.elr, g.emd, g.apd, g.p, g.pt, g.o, g.pn, g.b,
    g.tt, g.tm, g.ch, g.j, g.pg, g.et, g.exd, g.pvad, g.ag,
    _now AS ingested_at
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
        -- ch может прийти разным между дублями внутри одного батча — аккуратно объединяем:
        arraySort(arrayDistinct(arrayFlatten(groupArray(ch)))) AS ch,
        any(j)    AS j,
        any(pg)   AS pg,
        any(et)   AS et,
        any(exd)  AS exd,
        any(pvad) AS pvad,
        any(ag)   AS ag
    FROM stg.daily_codes_history_all
    GROUP BY c, t, opd
) AS g
LEFT JOIN stg.daily_codes_history AS f USING (c, t, opd)
WHERE f.c IS NULL
;

-- =====================================================================
-- Готово. После этого:
--   * ETL делает только INSERT INTO stg.daily_codes_history_all (...).
--   * MV автоматически переносит уникальные по (c,t,opd) строки в stg.daily_codes_history.
--   * Повторные прогоны ETL того же окна не создадут дублей в финальной таблице.
-- =====================================================================