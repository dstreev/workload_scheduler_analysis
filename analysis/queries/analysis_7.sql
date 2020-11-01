USE ${DB};

---------------------------------------------------------------
-- Query #7
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
