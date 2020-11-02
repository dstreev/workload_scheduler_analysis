USE ${DB};

-------------------------------------------------------------------------
-- Query #11
-- Categorize TEZ Workloads for Interactive
-- This query depends on Tez Application Tags supporting userid...
-------------------------------------------------------------------------
WITH
    TEZ_JOBS AS (
        SELECT
            REPORTING_TS,
            QUEUE,
--             USER_,
            ELAPSED_TIME,
            APPLICATION_TYPE AS TYPE,
            TRIM(AT_VALUE)   AS USER_
        FROM
            APP LATERAL VIEW EXPLODE(STR_TO_MAP(APPLICATION_TAGS, ",", "=")) ATTAGS AS AT_KEY, AT_VALUE
        WHERE
              TRIM(AT_KEY) = "userid"
          AND APPLICATION_TYPE = "TEZ"
          AND QUEUE != "default"
          AND REPORTING_TS LIKE "${RPT_DT}%")
        ,
    TEZ_JOB_QUEUE_WORKTIME AS (
        SELECT
--             SUBSTRING(REPORTING_TS, 0, 10) AS REPORTING_DT,
QUEUE,
SUM(ELAPSED_TIME) AS QUEUE_TOTAL_TEZ_TIME
        FROM
            TEZ_JOBS
        GROUP BY
            QUEUE
    ),
    AGGREGATE_BASE AS (SELECT
                           TJ.REPORTING_TS,
                           TJ.QUEUE,
                           TJ.USER_,
                           TJQW.QUEUE_TOTAL_TEZ_TIME,
                           TJ.ELAPSED_TIME,
                           TJ.ELAPSED_TIME DIV 30000 AS 30SEC_MULTIPLIER,
                           1                         AS ACCUMULATOR
                       FROM
                           TEZ_JOBS TJ
                               INNER JOIN TEZ_JOB_QUEUE_WORKTIME TJQW ON
                               TJ.QUEUE = TJQW.QUEUE
                       WHERE
                           TJ.ELAPSED_TIME > 0
    )
SELECT
    SUBSTRING(AB.REPORTING_TS, 0, 10)                          AS REPORTING_DT,
    QUEUE,
    ROUND(QUEUE_TOTAL_TEZ_TIME / 1000 / 60, 3)                 AS QUEUE_TTL_TEZ_TIME_MINS,
--     USER_,
    ((30SEC_MULTIPLIER * 0.5) + 0.5)                           AS MINS,
    SUM(ACCUMULATOR)                                           AS JobCount,
    SUM(ELAPSED_TIME)                                          AS TTL_TIME,
    ROUND(SUM(ELAPSED_TIME) / 1000 / 60, 3)                    AS TTL_TIME_MINS,
    ROUND((SUM(ELAPSED_TIME) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS PCT_OF_QUEUE
FROM
    AGGREGATE_BASE AB
WHERE
    ((30SEC_MULTIPLIER * 0.5) + 0.5) <= 3.0 -- Under 3 minutes
GROUP BY
    SUBSTRING(AB.REPORTING_TS, 0, 10),
    QUEUE,
    QUEUE_TOTAL_TEZ_TIME,
--     USER_,
    30SEC_MULTIPLIER
ORDER BY
    REPORTING_DT,
    QUEUE,
    MINS;
-- ,  USER_;

