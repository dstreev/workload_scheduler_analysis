USE ${DB};

---------------------------------------------------------------
-- Query #6
-- LOST Opportunities
-- When there are 'pending' containers in the cluster, what is the LOST opportunity of the cluster
-- if we could find a way run those containers.
---------------------------------------------------------------
SELECT
    SUBSTRING(EUVA.REPORTING_TS, 0, 16) AS REPORTING_MN,
    ACTUAL_USED,
    ESTIMATED_USED,
    CASE
        WHEN (ESTIMATED_USED > 100) THEN
            ROUND(100 - ACTUAL_USED, 2)
        ELSE
            ROUND(ESTIMATED_USED - ACTUAL_USED, 2)
        END                             AS LOST_OPPORTUNITY
FROM
    ESTIMATED_UTILIZATION_VS_ACTUAL EUVA
WHERE
    EUVA.REPORTING_TS LIKE "${RPT_DT}%"
ORDER BY
    REPORTING_MN;
