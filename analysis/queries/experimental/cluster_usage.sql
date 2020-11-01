USE ${DB};
-- Understand the ACTUAL use of the cluster.
--   Where resources are tight or loose, based on the queue usage.
-- Checking Content
SELECT
    DISTINCT REPORTING_TS
FROM
    QUEUE
WHERE
    REPORTING_TS LIKE "${RPT_DT}"
ORDER BY
    REPORTING_TS DESC;
    
SELECT
    DISTINCT REPORTING_TS
FROM
    QUEUE_USAGE
WHERE
    REPORTING_TS LIKE "${RPT_DT}"
ORDER BY
    REPORTING_TS DESC;
    
-- Actual Full Cluster Usage
WITH
    INTERVAL_USAGE AS
    (   SELECT
            SUBSTRING(REPORTING_TS, 0, 16) AS RPT_DT,
            --             SUM(ALLOCATED_CONTAINERS)             AS ALLOCATED_CONTAINERS,
            --             SUM(PENDING_CONTAINERS)               AS PENDING_CONTAINERS,
            ROUND(SUM(ABSOLUTE_CAPACITY), 2)      AS AVAILABLE,
            ROUND(SUM(ABSOLUTE_USED_CAPACITY), 2) AS USED
        FROM
            QUEUE
        WHERE
            REPORTING_TS LIKE "${RPT_DT}"
        GROUP BY
            SUBSTRING(REPORTING_TS, 0, 16)
    )
SELECT
    *
FROM
    INTERVAL_USAGE
ORDER BY
    RPT_DT;


-- Average Cluster Usage by 'Scale'
WITH
    INTERVAL_USAGE AS
    (   SELECT
            SUBSTRING(REPORTING_TS, 0, 16) AS RPT_MN,
            --             SUM(ALLOCATED_CONTAINERS)             AS ALLOCATED_CONTAINERS,
            --             SUM(PENDING_CONTAINERS)               AS PENDING_CONTAINERS,
            ROUND(SUM(ABSOLUTE_CAPACITY), 2)      AS AVAILABLE,
            ROUND(SUM(ABSOLUTE_USED_CAPACITY), 2) AS USED
        FROM
            QUEUE
        WHERE
            REPORTING_TS LIKE "${RPT_DT}"
        GROUP BY
            SUBSTRING(REPORTING_TS, 0, 16)
    )
    ,
    RPT_AVG AS
    (   SELECT
            RPT_MN,
            AVG(USED) AS USED
        FROM
            INTERVAL_USAGE
        GROUP BY
            RPT_MN
    )
SELECT
    --Scale: 10 for the day, 13 for Hour
    SUBSTRING(RPT_MN, 0, ${SCALE}) AS RPT_INTERVAL,
    ROUND(AVG(USED), 2)           AS USED
FROM
    RPT_AVG
GROUP BY
    SUBSTRING(RPT_MN, 0, ${SCALE})
ORDER BY
    RPT_INTERVAL;
-- Usage in Queue with number of users vs. the userlimit factor.
-- When there are pending containers
SELECT
    SUBSTRING(Q.REPORTING_TS, 0, 16) AS RPT_DT,
    Q.QUEUE_PATH,
    Q.QUEUE_NAME,
    Q.USER_LIMIT AS MIN_USER_LIMIT_PERCENT,
    Q.USER_LIMIT_FACTOR,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY,
    --     SUM(Q.ABSOLUTE_CAPACITY)         AS ABSOLUTE_CAPACITY,
    --     SUM(Q.ABSOLUTE_USED_CAPACITY)    AS ABSOLUTE_USED_CAPACITY,
    COUNT(U.USER_USERNAME) AS USER_COUNT
FROM
    QUEUE Q
LEFT OUTER JOIN
    QUEUE_USAGE U
ON
    Q.REPORTING_TS = U.REPORTING_TS
AND Q.QUEUE_PATH = U.QUEUE_PATH
AND Q.QUEUE_NAME = U.QUEUE_NAME
WHERE
    Q.REPORTING_TS LIKE "${RPT_DT}"
AND Q.PENDING_CONTAINERS > 0
GROUP BY
    SUBSTRING(Q.REPORTING_TS, 0, 16),
    Q.QUEUE_PATH,
    Q.QUEUE_NAME,
    Q.USER_LIMIT,
    Q.USER_LIMIT_FACTOR,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY
ORDER BY
    RPT_DT,
    QUEUE_PATH,
    QUEUE_NAME;
-- Cluster Utilization at points when there are "pending" container requests.
-- When there are pending containers, check for Queue full use. And identify how many
-- users are in the queue.  Mix together the configurations for User_Limits for comparison.
WITH
    BACKED_UP AS
    (   SELECT
            SUBSTRING(Q.REPORTING_TS, 0, 16) AS RPT_DT,
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.USER_LIMIT AS MIN_USER_LIMIT_PERCENT,
            Q.USER_LIMIT_FACTOR,
            Q.ALLOCATED_CONTAINERS,
            Q.PENDING_CONTAINERS,
            Q.ABSOLUTE_CAPACITY,
            Q.ABSOLUTE_USED_CAPACITY,
            COUNT(U.USER_USERNAME)       AS USER_COUNT,
            collect_set(U.USER_USERNAME) AS USERS
        FROM
            QUEUE Q
        LEFT OUTER JOIN
            QUEUE_USAGE U
        ON
            Q.REPORTING_TS = U.REPORTING_TS
        AND Q.QUEUE_PATH = U.QUEUE_PATH
        AND Q.QUEUE_NAME = U.QUEUE_NAME
        WHERE
            Q.REPORTING_TS LIKE "${RPT_DT}"
        AND Q.PENDING_CONTAINERS > 0
        AND Q.QUEUE_NAME LIKE "${QUEUE}"
               --     AND Q.ABSOLUTE_CAPACITY - Q.ABSOLUTE_USED_CAPACITY > 0
        GROUP BY
            SUBSTRING(Q.REPORTING_TS, 0, 16),
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.USER_LIMIT,
            Q.USER_LIMIT_FACTOR,
            Q.ALLOCATED_CONTAINERS,
            Q.PENDING_CONTAINERS,
            Q.ABSOLUTE_CAPACITY,
            Q.ABSOLUTE_USED_CAPACITY
    )
    ,
    INTERVAL_USAGE AS
    (   SELECT
            SUBSTRING(REPORTING_TS, 0, 16) AS RPT_DT,
            --             SUM(ALLOCATED_CONTAINERS)             AS ALLOCATED_CONTAINERS,
            --             SUM(PENDING_CONTAINERS)               AS PENDING_CONTAINERS,
            ROUND(SUM(ABSOLUTE_CAPACITY), 2)      AS AVAILABLE,
            ROUND(SUM(ABSOLUTE_USED_CAPACITY), 2) AS USED
        FROM
            QUEUE
        WHERE
            REPORTING_TS LIKE "${RPT_DT}"
        GROUP BY
            SUBSTRING(REPORTING_TS, 0, 16)
    )
SELECT
    B.RPT_DT,
    B.QUEUE_PATH,
    B.QUEUE_NAME,
    B.MIN_USER_LIMIT_PERCENT,
    B.USER_LIMIT_FACTOR,
    ROUND(B.ALLOCATED_CONTAINERS, 2) AS ALLOCATED_CONTAINERS,
    ROUND(B.PENDING_CONTAINERS,2) AS PENDING_CONTAINERS,
    ROUND(B.ABSOLUTE_CAPACITY, 2) AS ABSOLUTE_CAPACITY,
    ROUND(B.ABSOLUTE_USED_CAPACITY, 2) AS ABSOLUTE_USED_CAPACITY,
    B.USER_COUNT,
    B.USERS,
    ROUND(((B.ABSOLUTE_CAPACITY - B.ABSOLUTE_USED_CAPACITY) / B.ABSOLUTE_CAPACITY) * 100, 2) AS
              QUEUE_REMAINING_CAPACITY,
    I.USED AS TOTAL_CLUSTER_USAGE
FROM
    BACKED_UP B
INNER JOIN
    INTERVAL_USAGE I
ON
    B.RPT_DT = I.RPT_DT
    WHERE 
    B.ABSOLUTE_CAPACITY - B.ABSOLUTE_USED_CAPACITY > 0
ORDER BY
    B.RPT_DT,
    QUEUE_PATH,
    QUEUE_NAME;
-- Total Number of Concurrent Users during the 1 minute usage interval.
WITH
    CONCURRENCY AS
    (   SELECT
            SUBSTRING(U.REPORTING_TS, 0, 16)         AS RPT_DT,
            COUNT(U.USER_USERNAME)                   AS USER_COUNT,
            sort_array(collect_set(U.USER_USERNAME)) AS USERS
        FROM
            QUEUE_USAGE U
        GROUP BY
            SUBSTRING(U.REPORTING_TS, 0, 16)
    )
SELECT
    RPT_DT,
    USER_COUNT,
    SIZE(USERS) AS UNIQUE_USER_COUNT,
    USERS
FROM
    CONCURRENCY
WHERE
    RPT_DT LIKE "${RPT_DT}"
ORDER BY
    RPT_DT;
