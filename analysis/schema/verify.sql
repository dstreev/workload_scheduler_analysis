USE ${DB};

SHOW TABLES;
SHOW CREATE TABLE APP;
SHOW CREATE TABLE QUEUE;
SHOW CREATE TABLE QUEUE_USAGE;

-- SELECT * FROM HIERARCHY;

SELECT DISTINCT
    reporting_ts
FROM
    app
ORDER BY
    reporting_ts;

-- Validate the number of minutes captured for a day.
SELECT
    SUBSTRING(sub.REPORTING_MN, 0, 10) AS REPORTING_DT,
    COUNT(1)
FROM
    (SELECT DISTINCT
         substring(REPORTING_TS, 0, 16) AS REPORTING_MN
     FROM
         queue
     WHERE
         REPORTING_TS LIKE "${RPT_DT}%") sub
GROUP BY
    substring(sub.REPORTING_MN, 0, 10)
ORDER BY
    REPORTING_DT;

-- Validate the number of minutes captured for a day.
SELECT
    SUBSTRING(sub.REPORTING_MN, 0, 10) AS REPORTING_DT,
    COUNT(1)
FROM
    (SELECT DISTINCT
         substring(REPORTING_TS, 0, 16) AS REPORTING_MN
     FROM
         queue_usage
     WHERE
         REPORTING_TS LIKE "${RPT_DT}%") sub
GROUP BY
    substring(sub.REPORTING_MN, 0, 10)
ORDER BY
    REPORTING_DT;

-- Most current reporting ts
SELECT
    max(reporting_ts) AS LAST_TS_ENTRY
FROM
    queue_usage;

-- Cleanup junk.
DELETE
FROM
    queue_usage
WHERE
    reporting_ts NOT LIKE "2020%";

SELECT *
FROM
    queue
WHERE
    reporting_ts NOT LIKE "2020%";

-- Most current reporting ts
SELECT
    max(reporting_ts)
FROM
    queue_usage;

SELECT
distinct substring(REPORTING_TS, 0, 10)
-- *
FROM
    app
WHERE REPORTING_TS LIKE "${RPT_DT}%"
LIMIT 100;

SELECT *
FROM
    queue
LIMIT 10;

SELECT *
FROM
    queue_usage
LIMIT 10;

-- Counts by day of time elements QUEUE
SELECT
    SUBSTRING(REPORTING_MN, 0, 10) AS REPORTING_DT,
    COUNT(1)
FROM
    (
        SELECT DISTINCT
            SUBSTRING(REPORTING_TS, 0, 16) AS REPORTING_MN
        FROM
            QUEUE
        WHERE
            REPORTING_TS LIKE "${RPT_DT}%"
        GROUP BY
            SUBSTRING(REPORTING_TS, 0, 16)) sub
GROUP BY
    SUBSTRING(REPORTING_MN, 0, 10)
ORDER BY
    REPORTING_DT;

-- Counts by day of time elements QUEUE_USAGE
SELECT
    SUBSTRING(REPORTING_MN, 0, 10) AS REPORTING_DT,
    COUNT(1)
FROM
    (
        SELECT DISTINCT
            SUBSTRING(REPORTING_TS, 0, 16) AS REPORTING_MN
        FROM
            QUEUE_USAGE
        WHERE
                REPORTING_TS LIKE "${RPT_DT}%"
        GROUP BY
            SUBSTRING(REPORTING_TS, 0, 16)) sub
GROUP BY
    SUBSTRING(REPORTING_MN, 0, 10)
ORDER BY
    REPORTING_DT;

-- Check Sources for Overlap at the file level
SELECT
    SPLIT(INPUT__FILE__NAME, "app")[1],
    MIN(REPORTING_TS),
    MAX(REPORTING_TS),
    COUNT(1)
FROM
    ${DB}_source.APP
GROUP BY
    SPLIT(
    INPUT__FILE__NAME, "app")[
    1]
ORDER BY SPLIT(INPUT__FILE__NAME, "app")[1];

SELECT
    SPLIT(INPUT__FILE__NAME, "queue")[1],
    MIN(REPORTING_TS),
    MAX(REPORTING_TS),
    COUNT(1)
FROM
    ${DB}_source.QUEUE
GROUP BY
    SPLIT(
    INPUT__FILE__NAME, "queue")[
    1]
ORDER BY SPLIT(INPUT__FILE__NAME, "queue")[1];

SELECT
    SPLIT(INPUT__FILE__NAME, "queue_usage")[1],
    MIN(REPORTING_TS),
    MAX(REPORTING_TS),
    COUNT(1)
FROM
    ${DB}_source.QUEUE_USAGE
GROUP BY
    SPLIT(
    INPUT__FILE__NAME, "queue_usage")[
    1]
ORDER BY SPLIT(INPUT__FILE__NAME, "queue_usage")[1];