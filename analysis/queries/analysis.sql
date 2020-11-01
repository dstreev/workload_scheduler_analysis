USE ${DB};

-- Occurrences of Pending Containers in a Queue
-- SELECT
--     SUBSTRING(REPORTING_TS, 0, 10) AS REPORTING_DT,
--     COUNT(1)                       AS OCCURRENCES
-- FROM
--     QUEUE
-- WHERE
--     PENDING_CONTAINERS > 0
-- GROUP BY
--     SUBSTRING(REPORTING_TS, 0, 10)
-- ORDER BY
--     REPORTING_DT;


-- SELECT DISTINCT REPORTING_TS FROM QUEUE WHERE REPORTING_TS LIKE "${RPT_DT}";

-- Find the time frames where there is contention.
--   Pending Containers in a Queue
--   Compare the queues usage to the 'overall' cluster 'used' capacity.

-- This locates the timeframes where there are pending containers.
-- Then calculates the total used capacity for those time frames.
-- Then pulls in all the other queues in that time zone, along with
-- the usage 'users'.
-- Each of the timeframes will have a complete listing of all queue's in that
-- timeframe so we can compare the results of each to determine the capacity for
-- the backed up queue's can be increased.
-- With the list of users, we can also see if 'non-impersonation' is at play, which
-- might mean a change in 'ordering-policy'
WITH
    PENDING_REPORTING_TS AS (
        SELECT DISTINCT
            REPORTING_TS
        FROM
            QUEUE Q
        WHERE
              Q.REPORTING_TS LIKE "${RPT_DT}%"
          AND Q.PENDING_CONTAINERS > 0
          AND Q.QUEUE_NAME LIKE "${QUEUE}"
              -- Considering default queue a scratch, non-sla work area.  No guarantees.
          AND Q.QUEUE_NAME != "default"
    ),
    INTERVAL_USAGE AS
        -- The total absolute 'used' capacity at that point in time
        (SELECT
             REPORTING_TS,
             ROUND(SUM(ABSOLUTE_CAPACITY), 2)      AS AVAILABLE,
             ROUND(SUM(ABSOLUTE_USED_CAPACITY), 2) AS USED
         FROM
             QUEUE
         WHERE
             REPORTING_TS IN (SELECT REPORTING_TS FROM PENDING_REPORTING_TS)
         GROUP BY
             REPORTING_TS
        ),
    PENDING_QUEUES AS (
        SELECT
            SUBSTRING(Q.REPORTING_TS, 0, 16)    AS RPT_MN,
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.USER_LIMIT                        AS MIN_USER_LIMIT_PERCENT,
            Q.USER_LIMIT_FACTOR,
            CAST(Q.ALLOCATED_CONTAINERS AS INT) AS ALLOCATED_CONTAINERS,
            CAST(Q.PENDING_CONTAINERS AS INT)   AS PENDING_CONTAINERS,
            ROUND(Q.ABSOLUTE_CAPACITY, 2)       AS ABSOLUTE_CAPACITY,
            ROUND(Q.ABSOLUTE_USED_CAPACITY, 2)  AS ABSOLUTE_USED_CAPACITY,
            CASE
                WHEN (Q.PENDING_CONTAINERS > 0) THEN
                    ROUND((Q.ABSOLUTE_USED_CAPACITY / Q.ALLOCATED_CONTAINERS) * Q.PENDING_CONTAINERS, 2)
                ELSE
                    0
                END                             AS ADDITIONAL_REQUIREMENT,
            CASE
                WHEN (Q.PENDING_CONTAINERS > 0) THEN
                    ROUND(((Q.ABSOLUTE_USED_CAPACITY / Q.ALLOCATED_CONTAINERS) * Q.PENDING_CONTAINERS) +
                          Q.ABSOLUTE_USED_CAPACITY, 2)
                ELSE
                    ROUND(Q.ABSOLUTE_USED_CAPACITY, 2)
                END                             AS POTENTIAL_ABSOLUTE_NEEDED
        FROM
            QUEUE Q
        WHERE
              Q.REPORTING_TS LIKE "${RPT_DT}%"
          AND Q.PENDING_CONTAINERS > 0
          AND Q.QUEUE_NAME LIKE "${QUEUE}"
    ),
    TIMEBLOCKS_WITH_CONTENTION AS (
        SELECT
            SUBSTRING(Q.REPORTING_TS, 0, 16)   AS RPT_MN,
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.USER_LIMIT                       AS MIN_USER_LIMIT_PERCENT,
            Q.USER_LIMIT_FACTOR,
            Q.ALLOCATED_CONTAINERS,
            Q.PENDING_CONTAINERS,
            ROUND(Q.ABSOLUTE_CAPACITY, 2)      AS ABSOLUTE_CAPACITY,
            ROUND(Q.ABSOLUTE_USED_CAPACITY, 2) AS ABSOLUTE_USED_CAPACITY,
            CASE
                WHEN (Q.PENDING_CONTAINERS > 0) THEN
                    ROUND((Q.ABSOLUTE_USED_CAPACITY / Q.ALLOCATED_CONTAINERS) * Q.PENDING_CONTAINERS, 2)
                ELSE
                    0
                END                            AS ADDITIONAL_REQUIREMENT,
            CASE
                WHEN (Q.PENDING_CONTAINERS > 0) THEN
                    ROUND(((Q.ABSOLUTE_USED_CAPACITY / Q.ALLOCATED_CONTAINERS) * Q.PENDING_CONTAINERS) +
                          Q.ABSOLUTE_USED_CAPACITY, 2)
                ELSE
                    ROUND(Q.ABSOLUTE_USED_CAPACITY, 2)
                END                            AS POTENTIAL_ABSOLUTE_NEEDED,
            I.AVAILABLE,
            I.USED,
--             COUNT(U.USER_USERNAME)           AS USER_COUNT,
            COLLECT_SET(U.USER_USERNAME)       AS USERS
        FROM
            QUEUE Q
                INNER JOIN INTERVAL_USAGE I ON Q.REPORTING_TS = I.REPORTING_TS
                LEFT OUTER JOIN QUEUE_USAGE U ON Q.REPORTING_TS = U.REPORTING_TS AND
                                                 Q.QUEUE_PATH = U.QUEUE_PATH AND
                                                 Q.QUEUE_NAME = U.QUEUE_NAME
        WHERE
            Q.REPORTING_TS IN (SELECT REPORTING_TS FROM PENDING_REPORTING_TS)
        GROUP BY
            SUBSTRING(Q.REPORTING_TS, 0, 16),
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.USER_LIMIT,
            Q.USER_LIMIT_FACTOR,
            Q.ALLOCATED_CONTAINERS,
            Q.PENDING_CONTAINERS,
            Q.ABSOLUTE_CAPACITY,
            Q.ABSOLUTE_USED_CAPACITY,
            I.AVAILABLE,
            I.USED
    ),
    -- Where required capacity exceeds clusters capacity.
    EXCESS_OCCURRENCES_COUNT AS (SELECT
                                     SUB.RPT_MN,
                                     CASE
                                         WHEN (SUB.TOTAL_ABSOLUTE <= 100) THEN
                                             1
                                         ELSE
                                             0
                                         END UNDER_CAPACITY,
                                     CASE
                                         WHEN (SUB.TOTAL_ABSOLUTE <= 100) THEN
                                             0
                                         ELSE
                                             1
                                         END OVER_CAPACITY
                                 FROM
                                     (
                                         SELECT
                                             TBWC.RPT_MN,
                                             SUM(POTENTIAL_ABSOLUTE_NEEDED) TOTAL_ABSOLUTE
                                         FROM
                                             TIMEBLOCKS_WITH_CONTENTION TBWC
                                         GROUP BY TBWC.RPT_MN) SUB),
    ESTIMATED_UTILIZATION_VS_ACTUAL AS (
        -- When there are pending containers, if they COULD utilize the remaining cluster excess (not
        -- being used, WHAT would be the capacity levels.
        SELECT
            TBWC.RPT_MN,
            USED                                     AS ACTUAL_USED,
            ROUND(SUM(POTENTIAL_ABSOLUTE_NEEDED), 2) AS ESTIMATED_USED
        FROM
            TIMEBLOCKS_WITH_CONTENTION TBWC
        GROUP BY TBWC.RPT_MN, USED
    ),
    UNDER_CAP_WITH_PENDING AS (
        SELECT
            RPT_MN,
            QUEUE_PATH,
            QUEUE_NAME
        FROM
            TIMEBLOCKS_WITH_CONTENTION TBWC
        WHERE
              ABSOLUTE_USED_CAPACITY < (ABSOLUTE_CAPACITY * 0.95)
          AND PENDING_CONTAINERS > 0)


---------------------------------------------------------------
-- Query #1
-- Potential Excess Required Capacity Occurrences Summary (exceeds cluster capacity)
--  From a macro level this shows the overall effect of this on the cluster
---------------------------------------------------------------
SELECT
    SUBSTRING(OC.RPT_MN, 0, 10) AS REPORTING_DATE,
    SUM(OC.UNDER_CAPACITY)      AS UNDER_CAPACITY_COUNT,
    SUM(OC.OVER_CAPACITY)       AS OVER_CAPACITY_COUNT
FROM
    EXCESS_OCCURRENCES_COUNT OC
GROUP BY
    SUBSTRING(OC.RPT_MN, 0, 10)
ORDER BY
    REPORTING_DATE;

---------------------------------------------------------------
-- QUERY #2
-- Queues Running under capacity with Pending Containers
--  Indicates the queue is constrained and not allow all resources to be used.
-- Check user-limit-factor.
---------------------------------------------------------------
-- SELECT
--     SUBSTRING(RPT_MN, 0, 10) AS REPORTING_DT,
--     QUEUE_PATH,
--     QUEUE_NAME,
--     COUNT(1)                 AS OCCURRENCES
-- FROM
--     UNDER_CAP_WITH_PENDING
-- GROUP BY
--     SUBSTRING(RPT_MN, 0, 10), QUEUE_PATH, QUEUE_NAME
-- ORDER BY
--     REPORTING_DT, QUEUE_PATH, QUEUE_NAME;

---------------------------------------------------------------
-- Query #4
-- PENDING_QUEUES RAW
---------------------------------------------------------------
-- SELECT *
-- FROM
--     PENDING_QUEUES
-- ORDER BY
--     -- Bring the most egregious to the top
--     PENDING_CONTAINERS DESC;

---------------------------------------------------------------
-- Query #5
-- LOST Opportunities
-- When there are 'pending' containers in the cluster, what is the LOST opportunity of the cluster
-- if we could find a way run those containers.
---------------------------------------------------------------
-- SELECT
--     SUBSTRING(RPT_MN, 0, 10) AS REPORTING_DATE,
--     COUNT(1) AS INTERVALS,
--     ROUND(AVG(CASE
--                   WHEN (ESTIMATED_USED > 100) THEN
--                       ROUND(100 - ACTUAL_USED, 2)
--                   ELSE
--                       ROUND(ESTIMATED_USED - ACTUAL_USED, 2)
--         END), 2)             AS AVERAGE_LOST_OPPORTUNITY
-- FROM
--     ESTIMATED_UTILIZATION_VS_ACTUAL
-- GROUP BY
--     SUBSTRING(RPT_MN, 0, 10)
-- ORDER BY
--     REPORTING_DATE;

---------------------------------------------------------------
-- Query #6
-- LOST Opportunities
-- When there are 'pending' containers in the cluster, what is the LOST opportunity of the cluster
-- if we could find a way run those containers.
---------------------------------------------------------------
-- SELECT
--     RPT_MN,
--     ACTUAL_USED,
--     ESTIMATED_USED,
--     CASE
--         WHEN (ESTIMATED_USED > 100) THEN
--             ROUND(100 - ACTUAL_USED, 2)
--         ELSE
--             ROUND(ESTIMATED_USED - ACTUAL_USED, 2)
--         END AS LOST_OPPORTUNITY
-- FROM
--     ESTIMATED_UTILIZATION_VS_ACTUAL
-- ORDER BY
--     RPT_MN;


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
-- RAW DATA - Pending Queues
---------------------------------------------------------------
-- SELECT *
-- FROM
--     PENDING_QUEUES;

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

-- Look at the users in the above results.  We're looking for 'non-impersonation'
-- and user usage patterns.  Especially when in the same queue.


-- If there is 'remaining' cluster capacity in the above results.
-- then...
-- At that point in time, review the 'used' capacity of other queues.
-- Use this point in time to determine if the SLA requirements of other queues
--    is being pressed.
-- If there room left in SLA capacity, then we should be able to flex the queue.


---------------------------------------------------------------
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

-------------------------------------------------------------------------
-- Top 10 Application Failures in Queues running at or above Capacity
-------------------------------------------------------------------------
WITH
    TOP_ELAPSED_FAILED_APPS AS (
        SELECT
            A.REPORTING_TS,
            A.QUEUE,
            from_unixtime(CAST(A.STARTED_TIME / 1000 AS BIGINT))             AS STARTED_TIME,
            from_unixtime(CAST(A.FINISHED_TIME / 1000 AS BIGINT))            AS FINISHED_TIME,
            A.APPLICATION_TYPE,
            A.ELAPSED_TIME,
            A.FINAL_STATUS,
            A.ID,
            rank() OVER ( PARTITION BY A.QUEUE ORDER BY A.ELAPSED_TIME DESC) AS RNK
        FROM
            APP A
        WHERE
            A.FINAL_STATUS = "FAILED"
    )
SELECT *
FROM
    TOP_ELAPSED_FAILED_APPS TFA
WHERE
      TFA.REPORTING_TS LIKE "${RPT_DT}%"
  AND TFA.RNK < 11
ORDER BY
    TFA.QUEUE, TFA.RNK;

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

