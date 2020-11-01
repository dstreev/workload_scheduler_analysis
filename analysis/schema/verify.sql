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
DELETE FROM queue_usage where reporting_ts not like "2020%";

select * from queue where reporting_ts not like "2020%";

-- Most current reporting ts
SELECT
    max(reporting_ts)
FROM
    queue_usage;

SELECT *
FROM
    app
LIMIT 10;
SELECT *
FROM
    queue
LIMIT 10;
SELECT *
FROM
    queue_usage
LIMIT 10;
