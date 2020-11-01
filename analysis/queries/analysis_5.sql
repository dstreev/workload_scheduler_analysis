USE ${DB};

---------------------------------------------------------------
-- Query #5
-- LOST Opportunities
-- When there are 'pending' containers in the cluster, what is the LOST opportunity of the cluster
-- if we could find a way run those containers.
---------------------------------------------------------------
SELECT
    SUBSTRING(EUVA.REPORTING_TS, 0, 10) AS REPORTING_DT,
    COUNT(1)                            AS INTERVALS,
    ROUND(AVG(CASE
                  WHEN (ESTIMATED_USED > 100) THEN
                      ROUND(100 - ACTUAL_USED, 2)
                  ELSE
                      ROUND(ESTIMATED_USED - ACTUAL_USED, 2)
        END), 2)                        AS AVERAGE_LOST_OPPORTUNITY
FROM
    ESTIMATED_UTILIZATION_VS_ACTUAL EUVA
WHERE
    EUVA.REPORTING_TS LIKE "${RPT_DT}%"
GROUP BY
    SUBSTRING(EUVA.REPORTING_TS, 0, 10)
ORDER BY
    REPORTING_DT;

