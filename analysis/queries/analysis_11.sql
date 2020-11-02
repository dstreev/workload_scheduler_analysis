USE ${DB};

-------------------------------------------------------------------------
-- Query #11
-- Usage in Queue with number of users vs. the userlimit factor.
-- When there are pending containers
-------------------------------------------------------------------------
SELECT
    SUBSTRING(Q.REPORTING_TS, 0, 16) AS RPT_DT,
    Q.QUEUE_PATH,
    Q.QUEUE_NAME,
    Q.USER_LIMIT                     AS MIN_USER_LIMIT_PERCENT,
    Q.USER_LIMIT_FACTOR,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY,
    --     SUM(Q.ABSOLUTE_CAPACITY)         AS ABSOLUTE_CAPACITY,
    --     SUM(Q.ABSOLUTE_USED_CAPACITY)    AS ABSOLUTE_USED_CAPACITY,
    COUNT(U.USER_USERNAME)           AS USER_COUNT
FROM
    QUEUE Q
        LEFT OUTER JOIN
        QUEUE_USAGE U
        ON
                Q.REPORTING_TS = U.REPORTING_TS
                AND Q.QUEUE_PATH = U.QUEUE_PATH
                AND Q.QUEUE_NAME = U.QUEUE_NAME
WHERE
      Q.REPORTING_TS LIKE "${RPT_DT}%"
  AND Q.QUEUE_PATH LIKE "${QUEUE_PATH}%"
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
    QUEUE_PATH,
    QUEUE_NAME,
    RPT_DT;
