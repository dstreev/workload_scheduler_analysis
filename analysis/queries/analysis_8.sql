USE ${DB};

-------------------------------------------------------------------------
-- Query #8
-- Top 10 Application Failures in Queues
-------------------------------------------------------------------------
SELECT *
FROM
    TOP_ELAPSED_FAILED_APPS TFA
WHERE
        TFA.REPORTING_TS LIKE "${RPT_DT}%"
  AND TFA.RANK < 11
ORDER BY
    TFA.QUEUE, TFA.RANK;
