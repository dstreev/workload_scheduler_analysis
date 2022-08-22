USE ${DB};

-------------------------------------------------------------------------
-- Query #15
-- Grouping of 'repetitive' jobs with counts, times, and queue placement.
-------------------------------------------------------------------------
SELECT
    TO_DATE(REPORTING_TS)                                AS RPT_DT
  , A.QUEUE                                              AS QUEUE
  , A.APPLICATION_TYPE                                   AS APP_TYPE
  , A.NAME                                               AS JOB_NAME
  , ROUND(AVG((A.FINISHED_TIME - A.LAUNCH_TIME) / 1000)) AS AVG_JOB_LENGTH_SECS
  , ROUND(AVG((A.LAUNCH_TIME - A.STARTED_TIME) / 1000))  AS AVG_DELAY_SECS
  , ROUND(AVG(A.MEMORY_SECONDS))                         AS AVG_MEMORY_SECS
  , ROUND(AVG(A.VCORE_SECONDS))                          AS AVG_VCORE_SECS
  , COUNT(*)                                             AS JOB_COUNT
FROM
    APP A
WHERE
    REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    TO_DATE(REPORTING_TS)
  , A.QUEUE
  , A.APPLICATION_TYPE
  , A.NAME
HAVING
    JOB_COUNT > 20
ORDER BY
    RPT_DT, QUEUE, AVG_JOB_LENGTH_SECS;