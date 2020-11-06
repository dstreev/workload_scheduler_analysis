USE ${DB};

WITH
    APP_RANGE AS (
        SELECT
            MIN(RPT_MN) AS MIN_RPT_DT,
            MAX(RPT_MN) AS MAX_RPT_DT,
            COUNT(1) AS CYCLE_COUNT
        FROM
            (SELECT DISTINCT
                 SUBSTRING(REPORTING_TS, 0, 16) AS RPT_MN
             FROM
                 APP
             WHERE
                 REPORTING_TS LIKE "${RPT_DT}%") sub
    ),
    QUEUE_RANGE AS (
        SELECT
            MIN(RPT_MN) AS MIN_RPT_DT,
            MAX(RPT_MN) AS MAX_RPT_DT,
            COUNT(1) AS CYCLE_COUNT
        FROM
            (SELECT DISTINCT
                 SUBSTRING(REPORTING_TS, 0, 16) AS RPT_MN
             FROM
                 QUEUE
             WHERE
                     REPORTING_TS LIKE "${RPT_DT}%") sub
    ),
    QUEUE_USAGE_RANGE AS (
        SELECT
            MIN(RPT_MN) AS MIN_RPT_DT,
            MAX(RPT_MN) AS MAX_RPT_DT,
            COUNT(1) AS CYCLE_COUNT
        FROM
            (SELECT DISTINCT
                 SUBSTRING(REPORTING_TS, 0, 16) AS RPT_MN
             FROM
                 QUEUE_USAGE
             WHERE
                     REPORTING_TS LIKE "${RPT_DT}%") sub
    )
SELECT
    "APP",
    MIN_RPT_DT,
    MAX_RPT_DT,
    CYCLE_COUNT
FROM
    APP_RANGE
UNION ALL
SELECT
    "QUEUE",
    MIN_RPT_DT,
    MAX_RPT_DT,
    CYCLE_COUNT
FROM
    QUEUE_RANGE
UNION ALL
SELECT
    "QUEUE_USAGE",
    MIN_RPT_DT,
    MAX_RPT_DT,
    CYCLE_COUNT
FROM
    QUEUE_USAGE_RANGE;

