USE ${DB};

-- NOT WORKING WIP
SELECT
    ELAPSED_TIME,
    MEMORY_SECONDS,
    (MEMORY_SECONDS / ELAPSED_TIME * 1000 / 1024)                                                    AS MEM_SEC,
    ${CLUSTER_TOTAL_MEM}                                                                             AS CLUSTER_MEMORY,
    CAST(${CLUSTER_TOTAL_MEM} AS BIGINT) * (60 * 60 * 24)                                            AS CLUSTER_MEMORY_SECONDS_MB,
    (MEMORY_SECONDS) / (CAST(${CLUSTER_TOTAL_MEM} AS BIGINT) * (60 * 60 * 24)) AS X,
    *
FROM
    APP
WHERE
    REPORTING_TS LIKE "${RPT_DT}"
LIMIT 10;

-- NOT WORKING - WIP
WITH
    X AS (
        SELECT
            REPORTING_TS,
            ID,
            MEMORY_SECONDS,
            MEMORY_SECONDS * 1024 AS MEMORY_SECS_GB,
            ELAPSED_TIME,
            ${CLUSTER_TOTAL_MEM}                                    AS CLUSTER_MEMORY,
            -- GB MEM PER DAY with SEC Intervals
            CAST(${CLUSTER_TOTAL_MEM} AS BIGINT) * (60 * 60 * 24)   AS MB_DAILY_MEM_SECS,
            (MEMORY_SECONDS * 1024 /
            (CAST(${CLUSTER_TOTAL_MEM} AS BIGINT) * (60 * 60 * 24))) AS PERCENT_OF_DAILY_CLUSTER
        FROM
            APP
    )
SELECT
    SUM(PERCENT_OF_DAILY_CLUSTER)
FROM
    X
WHERE
    REPORTING_TS LIKE "${RPT_DT}";

-- TEZ JOBS (identified by 'successfulDAGS')
WITH
    USER_TEZ_CONSUMPTION AS (
        SELECT
            REPORTING_TS,
            USER_,
            QUEUE,
            ELAPSED_TIME,
            APPLICATION_TYPE               AS TYPE,
            MEMORY_SECONDS,
            VCORE_SECONDS,
--             TRIM(DIAG_KEY)   AS DIAG_KEY,
            CAST(TRIM(DIAG_VALUE) AS INT)  AS DIAG_VALUE
        FROM
            APP LATERAL VIEW EXPLODE(STR_TO_MAP(SPLIT(DIAGNOSTICS, ":")[1], ",", "=")) DTAGS AS DIAG_KEY, DIAG_VALUE
        WHERE
            TRIM(DIAG_KEY) = "successfulDAGs"
    ),
    USER_NON_TEZ_CONSUMPTION AS (
        SELECT
            REPORTING_TS,
            USER_,
            QUEUE,
            ELAPSED_TIME,
            APPLICATION_TYPE               AS TYPE,
            MEMORY_SECONDS,
            VCORE_SECONDS,
            1                              AS DIAG_VALUE
        FROM
            APP
        WHERE
            APPLICATION_TYPE != "TEZ"
    ),
    COMBINED_CONSUMPTION AS (
        SELECT *
        FROM
            USER_TEZ_CONSUMPTION
        UNION ALL
        SELECT *
        FROM
            USER_NON_TEZ_CONSUMPTION
    )

----------------------------------------------------------------------------------------
-- List of Top Consumer by Memory Consumption, by Day
----------------------------------------------------------------------------------------
SELECT
    substring(CC.REPORTING_TS, 0, 10) AS REPORTING_DT,
    CC.USER_,
    CC.QUEUE,
    CC.TYPE,
    COUNT(1)                                                                          AS NUM_OF_APPS,
    -- Reduce Memory  MB Seconds to GB Seconds, than calc the Minute Interval
    CC.MEMORY_SECONDS,
    CC.ELAPSED_TIME,
    ${CLUSTER_TOTAL_MEM}                                                              AS CLUSTER_TOTAL_MEM,
    ROUND(((SUM(CC.MEMORY_SECONDS) / 1073741824) / 60), 3)                            AS JOB_TOTAL_GB_MEMORY_MINUTES,
--     ((SUM(CC.MEMORY_SECONDS) / 1073741824)/60) / ${CLUSTER_TOTAL_MEM} AS CONSUMED_CLUSTER_MINUTES,
    ((SUM(CC.MEMORY_SECONDS) / (SUM(CC.ELAPSED_TIME) / 1000)) / ${CLUSTER_TOTAL_MEM}) AS CONSUMED_CLUSTER_MEMORY,
    ROUND((SUM(CC.VCORE_SECONDS) / 60), 3)                                            AS TOTAL_VCORE_MINUTES,
    ${HOST_VCORE_SCALE}                                                               AS HOST_VCORE_SCALE,
    ROUND(((SUM(CC.VCORE_SECONDS) / 60) / ${HOST_VCORE_SCALE}), 3)                    AS TOTAL_VCORE_HOST_MINUTES,
    ROUND(((SUM(CC.VCORE_SECONDS) / 60) / ${HOST_VCORE_SCALE} / 60), 3)               AS TOTAL_VCORE_HOST_HOURS,
--     SUM(CC.VCORE_SECONDS)  AS TOTAL_VCORE_SECONDS,
    SUM(CC.DIAG_VALUE)                                                                AS NUM_OF_DAGS,
    ROUND(AVG(CC.DIAG_VALUE), 2)                                                      AS AVG_DAGS_PER_APP
FROM
    COMBINED_CONSUMPTION CC
WHERE
    CC.REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    substring(CC.REPORTING_TS, 0, 10), USER_, QUEUE, TYPE, MEMORY_SECONDS, ELAPSED_TIME
ORDER BY
    REPORTING_DT, JOB_TOTAL_GB_MEMORY_MINUTES DESC;


-- Template
WITH
    USER_CONSUMPTION AS (
        SELECT
            substring(REPORTING_TS, 0, 10) AS RPT_DT,
            USER_,
            QUEUE,
            APPLICATION_TYPE               AS TYPE,
            MEMORY_SECONDS,
            VCORE_SECONDS,
            TRIM(DIAG_KEY)                 AS DIAG_KEY,
            TRIM(DIAG_VALUE)               AS DIAG_VALUE
        FROM
            APP LATERAL VIEW EXPLODE(STR_TO_MAP(SPLIT(DIAGNOSTICS, ":")[1], ",", "=")) DTAGS AS DIAG_KEY, DIAG_VALUE
    )
SELECT
    RPT_DT,
    USER_,
    QUEUE,
    TYPE,
    COUNT(1)            AS NUM_OF_APPS,
    SUM(MEMORY_SECONDS) AS TOTAL_MEMORY_SECONDS,
    SUM(VCORE_SECONDS)  AS TOTAL_VCORE_SECONDS,
    SUM(DIAG_VALUE)     AS NUM_OF_DAGS,
    AVG(DIAG_VALUE)     AS AVG_DAGS_PER_APP
FROM
    USER_CONSUMPTION
WHERE
      RPT_DT LIKE ${DT}
  AND DIAG_KEY = "successfulDAGs"
GROUP BY
    RPT_DT, USER_, QUEUE, TYPE
ORDER BY
    TOTAL_MEMORY_SECONDS DESC;





