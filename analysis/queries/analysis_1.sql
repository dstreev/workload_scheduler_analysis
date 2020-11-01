USE ${DB};

---------------------------------------------------------------
-- Query #1
-- Potential Excess Required Capacity Occurrences Summary (exceeds cluster capacity)
--  From a macro level this shows the overall effect of this on the cluster
---------------------------------------------------------------
SELECT
    SUBSTRING(OC.REPORTING_TS, 0, 10) AS REPORTING_DATE,
    SUM(OC.UNDER_CAPACITY)            AS UNDER_CAPACITY_COUNT,
    SUM(OC.OVER_CAPACITY)             AS OVER_CAPACITY_COUNT
FROM
    EXCESS_OCCURRENCES_COUNT OC
WHERE
    OC.REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    SUBSTRING(OC.REPORTING_TS, 0, 10)
ORDER BY
    REPORTING_DATE;

