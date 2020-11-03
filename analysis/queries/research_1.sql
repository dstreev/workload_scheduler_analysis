USE ${DB};

---------------------------------------------------------------
-- Research Query: #1
-- Applications Running in 'queue' during 'time'
---------------------------------------------------------------
SELECT
    Q.REPORTING_TS,
    A.QUEUE,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY,
    A.APPLICATION_TYPE,
    A.FINAL_STATUS,
    COUNT(1)                                  AS JOB_COUNT,
    SUM(A.ELAPSED_TIME)                       AS TOTAL_ELAPSED_JOB_TIME,
    ROUND(SUM(A.ELAPSED_TIME) / 1000 / 60, 2) AS CUMMULATIVE_RUNTIME_MINS,
    COLLECT_LIST(A.ID)                        AS APP_IDS,
    COLLECT_LIST(A.USER_)                     AS USERS,
    COLLECT_LIST(A.NAME)                      AS APP_NAMES,
    COLLECT_LIST(A.APPLICATION_TAGS)          AS APP_TAGS
FROM
    APP A
        INNER JOIN QUEUE Q
                   ON A.QUEUE = Q.QUEUE_NAME
WHERE
        CAST(A.STARTED_TIME AS BIGINT) < unix_timestamp("${RPT_MN}", "yyyy-MM-dd HH:mm") * 1000
  AND   CAST(A.FINISHED_TIME AS BIGINT) > unix_timestamp("${RPT_MN}", "yyyy-MM-dd HH:mm") * 1000
  AND   A.QUEUE LIKE "${QUEUE}"
  AND   Q.REPORTING_TS LIKE "${RPT_MN}%"
GROUP BY
    Q.REPORTING_TS,
    A.QUEUE,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY,
    A.APPLICATION_TYPE,
    A.FINAL_STATUS
ORDER BY
    A.QUEUE;
