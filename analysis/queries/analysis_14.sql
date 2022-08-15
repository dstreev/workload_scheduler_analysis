use ${DB};

-------------------------------------------------------------------------
-- Query #14
-- Delayed Launch Times
-------------------------------------------------------------------------
SELECT
    to_date(reporting_ts)           AS RPT_DT
  , application_type                AS TYPE
  , queue
  , COUNT(1)                        AS CNT
  , ROUND(AVG(launch_time - started_time)) AS DELAY_IN_MS
FROM
    app a
WHERE
    launch_time - started_time > 10000
    AND REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY to_date(reporting_ts)
       , application_type
       , queue
;