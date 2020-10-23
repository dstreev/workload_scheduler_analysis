USE ${DB};

select * from app limit 10;

-- TEZ JOBS (identified by the 'successfulDAGS')
WITH
    USER_TEZ_CONSUMPTION AS (
        SELECT
            substring(REPORTING_TS, 0, 10) AS RPT_DT,
            USER_,
            QUEUE,
            APPLICATION_TYPE               AS TYPE,
            MEMORY_SECONDS,
            VCORE_SECONDS,
--             TRIM(DIAG_KEY)   AS DIAG_KEY,
            CAST(TRIM(DIAG_VALUE) AS INT)              AS DIAG_VALUE
        FROM
            APP LATERAL VIEW EXPLODE(STR_TO_MAP(SPLIT(DIAGNOSTICS, ":")[1], ",", "=")) DTAGS AS DIAG_KEY, DIAG_VALUE
        WHERE
            TRIM(DIAG_KEY) = "successfulDAGs"
    ),
    USER_NON_TEZ_CONSUMPTION AS (
        SELECT
            substring(REPORTING_TS, 0, 10) AS RPT_DT,
            USER_,
            QUEUE,
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
    COMBINED_CONSUMPTION
WHERE
    RPT_DT LIKE ${DT}
GROUP BY
    RPT_DT, USER_, QUEUE, TYPE
ORDER BY
    RPT_DT, TOTAL_MEMORY_SECONDS DESC;


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





