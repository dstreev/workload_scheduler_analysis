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
            SUM(ELAPSED_TIME)              AS QUEUE_TOTAL_TEZ_TIME
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
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 0 AND TJ.ELAPSED_TIME <= 10000) THEN
                                   1
                               ELSE
                                   0
                               END AS 0_10,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 0 AND TJ.ELAPSED_TIME <= 10000) THEN
                                   TJ.ELAPSED_TIME
                               ELSE
                                   0
                               END AS 0_10_ET,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 10000 AND TJ.ELAPSED_TIME <= 30000) THEN
                                   1
                               ELSE
                                   0
                               END AS 10_30,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 10000 AND TJ.ELAPSED_TIME <= 30000) THEN
                                   TJ.ELAPSED_TIME
                               ELSE
                                   0
                               END AS 10_30_ET,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 30000 AND TJ.ELAPSED_TIME <= 60000) THEN
                                   1
                               ELSE
                                   0
                               END AS 30_60,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 30000 AND TJ.ELAPSED_TIME <= 60000) THEN
                                   TJ.ELAPSED_TIME
                               ELSE
                                   0
                               END AS 30_60_ET,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 60000) THEN
                                   1
                               ELSE
                                   0
                               END AS 60_ABOVE,
                           CASE
                               WHEN (TJ.ELAPSED_TIME > 60000) THEN
                                   TJ.ELAPSED_TIME
                               ELSE
                                   0
                               END AS 60_ABOVE_ET
                       FROM
                           TEZ_JOBS TJ
                               INNER JOIN TEZ_JOB_QUEUE_WORKTIME TJQW ON
                               TJ.QUEUE = TJQW.QUEUE
                       WHERE
                           TJ.ELAPSED_TIME > 0
    )
-- SELECT
--     SUBSTRING(AB.REPORTING_TS, 0, 10)                      AS REPORTING_DT,
--     QUEUE,
--     ROUND(QUEUE_TOTAL_TEZ_TIME / 1000 / 60, 3)             AS QUEUE_TTL_TEZ_TIME_MINS,
--     USER_,
--     SUM(0_10)                                              AS 0_10_JobCount,
-- --     SUM(0_10_ET)                     AS Category_0_10_Time,
--     ROUND(SUM(0_10_ET) / 1000 / 60, 3)                     AS 0_10_Aggregate_Time_in_Mins,
--     ROUND((SUM(0_10_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3)  AS 0_10_PCT_OF_QUEUE,
--     SUM(10_30)                                             AS 10_30_JobCount,
-- --     SUM(10_30_ET)                               AS Category_10_30_Time,
--     ROUND(SUM(10_30_ET) / 1000 / 60, 3)                    AS 10_30_Aggregate_Time_in_Mins,
--     ROUND((SUM(10_30_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS 10_30_PCT_OF_QUEUE,
--     SUM(30_60)                                             AS 30_60_JobCount,
-- --     SUM(30_60_ET)                               AS Category_30_60_Time,
--     ROUND(SUM(30_60_ET) / 1000 / 60, 3)                    AS 30_60_Aggregate_Time_in_Mins,
--     ROUND((SUM(30_60_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS 30_60_PCT_OF_QUEUE,
--     SUM(60_ABOVE)                                          AS 60_ABOVE_JobCount,
-- --     SUM(60_ABOVE_ET)                            AS Category_60_ABOVE_Time,
--     ROUND(SUM(60_ABOVE_ET) / 1000 / 60, 3)                 AS 60_ABOVE_Aggregate_Time_in_Mins,
--     ROUND((SUM(60_ABOVE_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS 60_ABOVE_PCT_OF_QUEUE
-- FROM
--     AGGREGATE_BASE AB
-- -- WHERE
-- --     AB.REPORTING_TS LIKE "${RPT_DT}%"
-- GROUP BY
--     SUBSTRING(AB.REPORTING_TS, 0, 10),
--     QUEUE,
--     QUEUE_TOTAL_TEZ_TIME,
--     USER_
-- ORDER BY
--     REPORTING_DT,
--     QUEUE,
--     USER_;



SELECT
    SUBSTRING(AB.REPORTING_TS, 0, 10)                      AS REPORTING_DT,
    QUEUE,
    ROUND(QUEUE_TOTAL_TEZ_TIME / 1000 / 60, 3)             AS QUEUE_TTL_TEZ_TIME_MINS,
    USER_,
    SUM(0_10)                                              AS 0_10_JobCount,
--     SUM(0_10_ET)                     AS Category_0_10_Time,
    ROUND(SUM(0_10_ET) / 1000 / 60, 3)                     AS 0_10_Aggregate_Time_in_Mins,
    ROUND((SUM(0_10_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3)  AS 0_10_PCT_OF_QUEUE,
    SUM(10_30)                                             AS 10_30_JobCount,
--     SUM(10_30_ET)                               AS Category_10_30_Time,
    ROUND(SUM(10_30_ET) / 1000 / 60, 3)                    AS 10_30_Aggregate_Time_in_Mins,
    ROUND((SUM(10_30_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS 10_30_PCT_OF_QUEUE,
    SUM(30_60)                                             AS 30_60_JobCount,
--     SUM(30_60_ET)                               AS Category_30_60_Time,
    ROUND(SUM(30_60_ET) / 1000 / 60, 3)                    AS 30_60_Aggregate_Time_in_Mins,
    ROUND((SUM(30_60_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS 30_60_PCT_OF_QUEUE,
    SUM(60_ABOVE)                                          AS 60_ABOVE_JobCount,
--     SUM(60_ABOVE_ET)                            AS Category_60_ABOVE_Time,
    ROUND(SUM(60_ABOVE_ET) / 1000 / 60, 3)                 AS 60_ABOVE_Aggregate_Time_in_Mins,
    ROUND((SUM(60_ABOVE_ET) / QUEUE_TOTAL_TEZ_TIME) * 100, 3) AS 60_ABOVE_PCT_OF_QUEUE
FROM
    AGGREGATE_BASE AB
-- WHERE
--     AB.REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    SUBSTRING(AB.REPORTING_TS, 0, 10),
    QUEUE,
    QUEUE_TOTAL_TEZ_TIME,
    USER_
ORDER BY
    REPORTING_DT,
    QUEUE,
    USER_;

SELECT QUEUE, ROUND(QUEUE_TOTAL_TEZ_TIME / 1000 / 60, 3) AS QUEUE_TTL_TEZ_TIME FROM TEZ_JOB_QUEUE_WORKTIME;


SELECT 203 DIV 100;