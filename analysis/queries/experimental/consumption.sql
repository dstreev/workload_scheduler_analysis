USE ${DB};

-- We're collecting the App data every hour.  So the analysis should be specific for that hour
-- interval, since we aren't getting the applications start,launch,or end times.


WITH
    MEMORY_TOTALS_BY_HOUR AS (
        SELECT
            SUBSTRING(A.REPORTING_TS, 0, 13) AS RPT_HOUR,
            SUM(A.MEMORY_SECONDS)            AS SUM_OF_MEMORY_SECONDS
        FROM
            APP A
        WHERE
              A.REPORTING_TS LIKE ${RPT_DT}
          AND A.QUEUE LIKE ${QUEUE}
        GROUP BY
            SUBSTRING(A.REPORTING_TS, 0, 13)
    ),
    QUEUE_MEMORY_BY_HOUR AS (
        SELECT
            SUBSTRING(A.REPORTING_TS, 0, 13) AS RPT_HOUR,
            A.QUEUE,
            SUM(A.MEMORY_SECONDS)            AS SUM_OF_MEMORY_SECONDS
        FROM
            APP A
        WHERE
              A.REPORTING_TS LIKE ${RPT_DT}
          AND A.QUEUE LIKE ${QUEUE}
        GROUP BY
            SUBSTRING(A.REPORTING_TS, 0, 13),
            A.QUEUE
    ),
    JOINED_HOURLY_MEM AS (
        SELECT
            Q.RPT_HOUR,
            Q.QUEUE,
            Q.SUM_OF_MEMORY_SECONDS                                             AS QUEUE_MEMORY,
            M.SUM_OF_MEMORY_SECONDS                                             AS TOTAL_MEMORY,
            ROUND((Q.SUM_OF_MEMORY_SECONDS / M.SUM_OF_MEMORY_SECONDS) * 100, 5) AS PERCENT_OF_TOTAL_USED
        FROM
            QUEUE_MEMORY_BY_HOUR Q
                JOIN MEMORY_TOTALS_BY_HOUR M ON Q.RPT_HOUR = M.RPT_HOUR
    )
SELECT *
FROM
    JOINED_HOURLY_MEM
ORDER BY
    RPT_HOUR, PERCENT_OF_TOTAL_USED DESC, QUEUE;

SELECT
    RPT_HOUR,
    SUM(PERCENT_OF_TOTAL_USED)
FROM
    JOINED_HOURLY_MEM
GROUP BY
    RPT_HOUR;



SELECT
    SUBSTRING(A.REPORTING_TS, 0, 13)            AS RPT_HOUR,
    sort_array(collect_set(A.APPLICATION_TYPE)) AS APP_TYPES,
    A.QUEUE,
    SUM(A.MEMORY_SECONDS)                       AS SUM_OF_MEMORY_SECONDS,
    SUM(A.VCORE_SECONDS)                        AS SUM_OF_VCORE_SECONDS,
    sort_array(collect_set(A.USER_))            AS USERS
FROM
    APP A
WHERE
    REPORTING_TS LIKE ${RPT_DT}
GROUP BY
    SUBSTRING(A.REPORTING_TS, 0, 13),
    APPLICATION_TYPE,
    QUEUE;

-- Using 'apps', summarize 'actual'

-- misc
SELECT
    SUBSTRING(A.REPORTING_TS, 0, 13) AS RPT_HR,
    COUNT(*)
FROM
    APP A
WHERE
    REPORTING_TS LIKE ${RPT_DT}
GROUP BY
    SUBSTRING(A.REPORTING_TS, 0, 13);

SELECT *
FROM
    APP
WHERE
    REPORTING_TS LIKE ${RPT_DT};