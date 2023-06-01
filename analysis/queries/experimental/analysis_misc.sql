USE ${DB};

---------------------------------------------------------------
-- Full Interval Cluster Used Metrics
---------------------------------------------------------------
SELECT
    SUBSTRING(IU.REPORTING_TS, 0, 16) AS REPORTING_MN,
    IU.AVAILABLE,
    IU.USED
FROM
    INTERVAL_USAGE IU
WHERE
    IU.REPORTING_TS LIKE "${RPT_DT}%"
ORDER BY
    REPORTING_MN;

---------------------------------------------------------------
-- RAW DATA TimeBlocks With Contention
---------------------------------------------------------------
-- SELECT
--     TBWC.*
-- FROM
--     TIMEBLOCKS_WITH_CONTENTION TBWC
-- ORDER BY
--     TBWC.RPT_MN, TBWC.QUEUE_PATH, TBWC.QUEUE_NAME;


---------------------------------------------------------------

---------------------------------------------------------------
-- SELECT
--     TBWC.RPT_MN,
--     TBWC.QUEUE_NAME,
--     TBWC.MIN_USER_LIMIT_PERCENT,
--     TBWC.USER_LIMIT_FACTOR,
--     TBWC.ALLOCATED_CONTAINERS,
--     TBWC.PENDING_CONTAINERS,
--     ROUND(TBWC.ABSOLUTE_CAPACITY, 2) as ABSOLUTE_CAPACITY,
--     ROUND(TBWC.ABSOLUTE_USED_CAPACITY) as ABSOLUTE_USED_CAPACITY,
--     size(TBWC.USERS) AS NUM_OF_USERS
-- FROM
--     TIMEBLOCKS_WITH_CONTENTION TBWC
-- WHERE
--         QUEUE_NAME LIKE "${QUEUE}"
-- ORDER BY
--     TBWC.RPT_MN, TBWC.QUEUE_PATH, TBWC.QUEUE_NAME;

---------------------------------------------------------------
-- Total event count for under cap with pending.
---------------------------------------------------------------
-- SELECT
--     COUNT(1)
-- FROM
--     UNDER_CAP_WITH_PENDING;

-- Look at the users in the above analysis.  We're looking for 'non-impersonation'
-- and user usage patterns.  Especially when in the same queue.


-- If there is 'remaining' cluster capacity in the above analysis.
-- then...
-- At that point in time, review the 'used' capacity of other queues.
-- Use this point in time to determine if the SLA requirements of other queues
--    is being pressed.
-- If there room left in SLA capacity, then we should be able to flex the queue.


SELECT *
FROM
    APP
WHERE
    ID = "${APP_ID}";

SELECT
    A.QUEUE,
    A.ID                                      AS APP_ID,
    A.APPLICATION_TYPE,
    MIN(Q.ABSOLUTE_CAPACITY)                  AS ABS_CAPACITY,
    MIN(Q.ABSOLUTE_USED_CAPACITY)             AS ABS_MIN_CAPACITY,
    MAX(Q.ABSOLUTE_USED_CAPACITY)             AS ABS_MAX_CAPACITY,
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
      A.FINAL_STATUS = "FAILED"
  AND A.REPORTING_TS LIKE "${RPT_DT}%";

SELECT *
FROM
    PENDING_QUEUES
WHERE
      REPORTING_TS LIKE "${RPT_DT}%"
  AND QUEUE_NAME LIKE "${QUEUE}%";



SELECT *
FROM
--     queue
queue_usage
WHERE
      reporting_ts LIKE "${RPT_DT}%"
  AND queue_name LIKE "${QUEUE}%";


SELECT
    from_unixtime(CAST(STARTED_TIME / 1000 AS BIGINT)),
    from_unixtime(CAST(FINISHED_TIME / 1000 AS BIGINT)),
    *
FROM
    app
WHERE
      REPORTING_TS LIKE "${RPT_DT}%"
  AND QUEUE LIKE "${QUEUE}%";


SELECT *
FROM
    queue_usage
WHERE
    reporting_ts LIKE "${RPT_DT}%"
LIMIT 10;
