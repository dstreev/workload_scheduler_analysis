USE ${DB};

---------------------------------------------------------------
-- QUERY #2
-- Queues Running under capacity with Pending Containers
--  Indicates the queue is constrained and not allow all resources to be used.
-- Check user-limit-factor.
---------------------------------------------------------------
SELECT
    SUBSTRING(REPORTING_TS, 0, 10) AS REPORTING_DT,
    QUEUE_PATH,
    QUEUE_NAME,
    COUNT(1)                       AS OCCURRENCES
FROM
    UNDER_CAP_WITH_PENDING UCP
WHERE
    UCP.REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    SUBSTRING(REPORTING_TS, 0, 10), QUEUE_PATH, QUEUE_NAME
ORDER BY
    REPORTING_DT, QUEUE_PATH, QUEUE_NAME;
