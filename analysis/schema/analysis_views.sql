USE ${DB};

-- Find the time frames where there is contention.
--   Pending Containers in a Queue
--   Compare the queues usage to the 'overall' cluster 'used' capacity.

-- This locates the timeframes where there are pending containers.
-- Then calculates the total used capacity for those time frames.
-- Then pulls in all the other queues in that time zone, along with
-- the usage 'users'.
-- Each of the timeframes will have a complete listing of all queue's in that
-- timeframe so we can compare the analysis of each to determine the capacity for
-- the backed up queue's can be increased.
-- With the list of users, we can also see if 'non-impersonation' is at play, which
-- might mean a change in 'ordering-policy'

DROP VIEW IF EXISTS PENDING_REPORTING_TS;
CREATE VIEW PENDING_REPORTING_TS AS
(
SELECT DISTINCT
    REPORTING_TS
FROM
    QUEUE Q
WHERE
--                 Q.REPORTING_TS LIKE "${RPT_DT}%"
--           AND
    Q.PENDING_CONTAINERS > 0
--           AND Q.QUEUE_NAME LIKE "${QUEUE}"
  -- Considering default queue a scratch, non-sla work area.  No guarantees.
  AND Q.QUEUE_NAME != "default"
    );

DROP VIEW IF EXISTS INTERVAL_USAGE;
CREATE VIEW INTERVAL_USAGE AS
    -- The total absolute 'used' capacity at that point in time
(
SELECT
    REPORTING_TS,
    ROUND(SUM(ABSOLUTE_CAPACITY), 2)      AS AVAILABLE,
    ROUND(SUM(ABSOLUTE_USED_CAPACITY), 2) AS USED
FROM
    QUEUE
GROUP BY
    REPORTING_TS
    );

DROP VIEW IF EXISTS INTERVAL_USAGE_WHEN_PENDING;
CREATE VIEW INTERVAL_USAGE_WHEN_PENDING AS
    -- The total absolute 'used' capacity at that point in time
(
SELECT
    REPORTING_TS,
    ROUND(SUM(ABSOLUTE_CAPACITY), 2)      AS AVAILABLE,
    ROUND(SUM(ABSOLUTE_USED_CAPACITY), 2) AS USED
FROM
    QUEUE
WHERE
    REPORTING_TS IN (SELECT REPORTING_TS FROM PENDING_REPORTING_TS)
GROUP BY
    REPORTING_TS
    );

DROP VIEW IF EXISTS PENDING_QUEUES;
CREATE VIEW PENDING_QUEUES AS
(
SELECT
    Q.REPORTING_TS,
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
--                 Q.REPORTING_TS LIKE "${RPT_DT}%"
--           AND
Q.PENDING_CONTAINERS > 0
--           AND Q.QUEUE_NAME LIKE "${QUEUE}"
    );

DROP VIEW IF EXISTS TIMEBLOCKS_WITH_CONTENTION;
CREATE VIEW TIMEBLOCKS_WITH_CONTENTION AS
(
SELECT
--             SUBSTRING(Q.REPORTING_TS, 0, 16)   AS RPT_MN,
Q.REPORTING_TS,
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
        INNER JOIN INTERVAL_USAGE_WHEN_PENDING I ON Q.REPORTING_TS = I.REPORTING_TS
        LEFT OUTER JOIN QUEUE_USAGE U ON Q.REPORTING_TS = U.REPORTING_TS AND
                                         Q.QUEUE_PATH = U.QUEUE_PATH AND
                                         Q.QUEUE_NAME = U.QUEUE_NAME
WHERE
    Q.REPORTING_TS IN (SELECT REPORTING_TS FROM PENDING_REPORTING_TS)
GROUP BY
--             SUBSTRING(Q.REPORTING_TS, 0, 16),
Q.REPORTING_TS,
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
    );


-- Where required capacity exceeds clusters capacity.
DROP VIEW IF EXISTS EXCESS_OCCURRENCES_COUNT;
CREATE VIEW EXCESS_OCCURRENCES_COUNT AS
(
SELECT
    SUB.REPORTING_TS,
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
            TBWC.REPORTING_TS,
            SUM(POTENTIAL_ABSOLUTE_NEEDED) TOTAL_ABSOLUTE
        FROM
            TIMEBLOCKS_WITH_CONTENTION TBWC
        GROUP BY TBWC.REPORTING_TS) SUB);

DROP VIEW IF EXISTS ESTIMATED_UTILIZATION_VS_ACTUAL;
CREATE VIEW ESTIMATED_UTILIZATION_VS_ACTUAL AS
(
-- When there are pending containers, if they COULD utilize the remaining cluster excess (not
-- being used, WHAT would be the capacity levels.
SELECT
    TBWC.REPORTING_TS,
    USED                                     AS ACTUAL_USED,
    ROUND(SUM(POTENTIAL_ABSOLUTE_NEEDED), 2) AS ESTIMATED_USED
FROM
    TIMEBLOCKS_WITH_CONTENTION TBWC
GROUP BY
    TBWC.REPORTING_TS, USED
    );

DROP VIEW IF EXISTS UNDER_CAP_WITH_PENDING;
CREATE VIEW UNDER_CAP_WITH_PENDING AS
(
SELECT
    REPORTING_TS,
    QUEUE_PATH,
    QUEUE_NAME
FROM
    TIMEBLOCKS_WITH_CONTENTION TBWC
WHERE
      ABSOLUTE_USED_CAPACITY < (ABSOLUTE_CAPACITY * 0.95)
  AND PENDING_CONTAINERS > 0);


---------------------------------------------------------------
-- TOP N - Failed Apps Ranked by Partition, Total Elapsed Time
---------------------------------------------------------------
DROP VIEW IF EXISTS TOP_ELAPSED_FAILED_APPS;
CREATE VIEW TOP_ELAPSED_FAILED_APPS AS
(
SELECT
    A.REPORTING_TS,
    A.QUEUE,
    from_unixtime(CAST(A.STARTED_TIME / 1000 AS BIGINT))             AS STARTED_TIME,
    from_unixtime(CAST(A.FINISHED_TIME / 1000 AS BIGINT))            AS FINISHED_TIME,
    A.APPLICATION_TYPE,
    A.ELAPSED_TIME,
    A.FINAL_STATUS,
    A.ID,
    rank() OVER ( PARTITION BY A.QUEUE ORDER BY A.ELAPSED_TIME DESC) AS RANK
FROM
    APP A
WHERE
    A.FINAL_STATUS = "FAILED"
    );