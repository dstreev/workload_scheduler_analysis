USE ${DB};

WITH
    PENDING_REPORTING_TS AS (
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
--     PENDING_QUEUES AS (
--         SELECT
--             SUBSTRING(Q.REPORTING_TS, 0, 16)    AS RPT_MN,
--             Q.QUEUE_PATH,
--             Q.QUEUE_NAME,
--             Q.USER_LIMIT                        AS MIN_USER_LIMIT_PERCENT,
--             Q.USER_LIMIT_FACTOR,
--             CAST(Q.ALLOCATED_CONTAINERS AS INT) AS ALLOCATED_CONTAINERS,
--             CAST(Q.PENDING_CONTAINERS AS INT)   AS PENDING_CONTAINERS,
--             ROUND(Q.ABSOLUTE_CAPACITY, 2)       AS ABSOLUTE_CAPACITY,
--             ROUND(Q.ABSOLUTE_USED_CAPACITY, 2)  AS ABSOLUTE_USED_CAPACITY,
--             CASE
--                 WHEN (Q.PENDING_CONTAINERS > 0) THEN
--                     ROUND((Q.ABSOLUTE_USED_CAPACITY / Q.ALLOCATED_CONTAINERS) * Q.PENDING_CONTAINERS, 2)
--                 ELSE
--                     0
--                 END                             AS ADDITIONAL_REQUIREMENT,
--             CASE
--                 WHEN (Q.PENDING_CONTAINERS > 0) THEN
--                     ROUND(((Q.ABSOLUTE_USED_CAPACITY / Q.ALLOCATED_CONTAINERS) * Q.PENDING_CONTAINERS) +
--                           Q.ABSOLUTE_USED_CAPACITY, 2)
--                 ELSE
--                     ROUND(Q.ABSOLUTE_USED_CAPACITY, 2)
--                 END                             AS POTENTIAL_ABSOLUTE_NEEDED
--         FROM
--             QUEUE Q
--         WHERE
--                 Q.REPORTING_TS LIKE "${RPT_DT}%"
--           AND Q.PENDING_CONTAINERS > 0
--           AND Q.QUEUE_NAME LIKE "${QUEUE}"
--     ),
    TIMEBLOCKS_WITH_CONTENTION AS (
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
                INNER JOIN INTERVAL_USAGE I ON Q.REPORTING_TS = I.REPORTING_TS
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
    ),
    -- Where required capacity exceeds clusters capacity.
    EXCESS_OCCURRENCES_COUNT AS (SELECT
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
                                         GROUP BY TBWC.REPORTING_TS) SUB),
    ESTIMATED_UTILIZATION_VS_ACTUAL AS (
        -- When there are pending containers, if they COULD utilize the remaining cluster excess (not
        -- being used, WHAT would be the capacity levels.
        SELECT
            TBWC.REPORTING_TS,
            USED                                     AS ACTUAL_USED,
            ROUND(SUM(POTENTIAL_ABSOLUTE_NEEDED), 2) AS ESTIMATED_USED
        FROM
            TIMEBLOCKS_WITH_CONTENTION TBWC
        GROUP BY TBWC.REPORTING_TS, USED
    ),
    UNDER_CAP_WITH_PENDING AS (
        SELECT
            REPORTING_TS,
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
    SUBSTRING(OC.REPORTING_TS, 0, 10) AS REPORTING_DATE,
    SUM(OC.UNDER_CAPACITY)      AS UNDER_CAPACITY_COUNT,
    SUM(OC.OVER_CAPACITY)       AS OVER_CAPACITY_COUNT
FROM
    EXCESS_OCCURRENCES_COUNT OC
WHERE
    OC.REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    SUBSTRING(OC.REPORTING_TS, 0, 10)
ORDER BY
    REPORTING_DATE;