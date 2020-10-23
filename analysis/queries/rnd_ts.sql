USE ${DB};

SELECT DISTINCT
    REPORTING_TS
FROM
    QUEUE
ORDER BY
    REPORTING_TS;

SELECT unix_timestamp('2020-10-13 20:48:03-0400');

SELECT
    concat(substring_index(substring_index(REPORTING_TS, '-', 3), ':', 2), ':00') AS RPT_TS,
    sum(ABSOLUTE_USED_CAPACITY)                                                   AS ABSOLUTE_USED_CAPACITY,
    sum(CAPACITY)                                                                 AS TOTAL_CAPACITY
FROM
    QUEUE
GROUP BY
    concat(substring_index(substring_index(REPORTING_TS, '-', 3), ':', 2), ':00')
ORDER BY
    concat(substring_index(substring_index(REPORTING_TS, '-', 3), ':', 2), ':00');

WITH
    APP AS (
        SELECT concat(substring_index(substring_index(REPORTING_TS, '-', 3), ':', 2), ':00'),
    )


SELECT *
FROM
    QUEUE_USAGE
LIMIT 10;

SELECT *
-- count(*)
FROM
    APP
WHERE
--       APPLICATION_TYPE != "TEZ"
--   AND
    REPORTING_TS LIKE ${RPT_DT}
LIMIT 2000;


-- DIAG Break down.
WITH
    DIAG AS (
        SELECT
            concat(substring_index(substring_index(REPORTING_TS, '-', 3), ':', 2), ':00') AS RPT_TS,
            ID                                                                            AS APP_ID,
            QUEUE,
            APPLICATION_TYPE,
            from_unixtime(CAST(STARTED_TIME / 1000 AS INT), "yyyy-MM-dd HH:mm:ss")        AS STARTED_TIME,
--             from_unixtime(CAST(LAUNCH_TIME / 1000 AS INT), "yyyy-MM-dd HH:mm:ss")         AS LAUNCH_TIME,
            ROUND((LAUNCH_TIME - STARTED_TIME) / 1000)                                    AS DELAYED,
            from_unixtime(CAST(FINISHED_TIME / 1000 AS INT), "yyyy-MM-dd HH:mm:ss")       AS FINISHED_TIME,
            ROUND(ELAPSED_TIME / 1000)                                                    AS ELAPSED_SECS,
            TRIM(DIAG_KEY)                                                                AS DIAG_KEY,
            TRIM(DIAG_VALUE)                                                              AS DIAG_VALUE,
--             APPTAG_KEY,
--             APPTAG_VALUE,
            MEMORY_SECONDS,
            VCORE_SECONDS
        FROM
            APP LATERAL VIEW EXPLODE(STR_TO_MAP(SPLIT(DIAGNOSTICS, ":")[1], ",", "=")) DTAGS AS DIAG_KEY, DIAG_VALUE
        WHERE
            APPLICATION_TYPE = "TEZ"
    )
/*
SELECT
    RPT_TS,
    APP_ID,
    STARTED_TIME,
--     LAUNCH_TIME,
    DELAYED,
    FINISHED_TIME,
    ELAPSED_SECS,
    DIAG_KEY,
    DIAG_VALUE,
    MEMORY_SECONDS,
    VCORE_SECONDS
FROM
    DIAG
LIMIT 10;
*/
SELECT
    DIAG_KEY,
    SUM(DIAG_VALUE) AS DIAG_SUM
FROM
    DIAG
GROUP BY
    DIAG_KEY;

-- Looking for delayed jobs with 'hive' user.
-- Requires Application Tags with userId.
-- Won't work when start,launch, and end times aren't available.  Usually the case
--    when ATS isn't running and capturing this data.
WITH
    APP_TAGGED AS (
        SELECT
            concat(substring_index(substring_index(REPORTING_TS, '-', 3), ':', 2), ':00') AS RPT_TS,
            ID                                                                            AS APP_ID,
            QUEUE,
            APPLICATION_TYPE,
            from_unixtime(CAST(STARTED_TIME / 1000 AS INT), "yyyy-MM-dd HH:mm:ss")        AS STARTED_TIME,
            from_unixtime(CAST(LAUNCH_TIME / 1000 AS INT), "yyyy-MM-dd HH:mm:ss")         AS LAUNCH_TIME,
            ROUND((LAUNCH_TIME - STARTED_TIME) / 1000)                                    AS DELAYED,
            from_unixtime(CAST(FINISHED_TIME / 1000 AS INT), "yyyy-MM-dd HH:mm:ss")       AS FINISHED_TIME,
            ROUND(ELAPSED_TIME / 1000)                                                    AS ELAPSED_SECS,
            APPTAG_KEY,
            APPTAG_VALUE,
            MEMORY_SECONDS,
            VCORE_SECONDS
        FROM
            APP LATERAL VIEW EXPLODE(STR_TO_MAP(APPLICATION_TAGS, ",", "=")) ATAGS AS APPTAG_KEY, APPTAG_VALUE
    )
SELECT
    RPT_TS,
    APP_ID,
    STARTED_TIME,
    LAUNCH_TIME,
    DELAYED,
    FINISHED_TIME,
    ELAPSED_SECS,
    APPTAG_VALUE AS HIVE_USER,
    MEMORY_SECONDS,
    VCORE_SECONDS
FROM
    APP_TAGGED
WHERE
      APPTAG_KEY = "userid"
  AND DELAYED > 2;


SELECT
    USER_,
    QUEUE,
    APPLICATION_TYPE,
    COUNT(1)            AS APP_COUNT,
    SUM(MEMORY_SECONDS) AS TOTAL_MEMORY_SECONDS,
    SUM(VCORE_SECONDS)  AS TOTAL_VCORE_SECONDS
FROM
    APP
GROUP BY
    USER_, QUEUE, APPLICATION_TYPE
ORDER BY
    TOTAL_MEMORY_SECONDS DESC;

SELECT *
FROM
    QUEUE
WHERE
    REPORTING_TS LIKE '2020-10-13 20:20%'
LIMIT 10;