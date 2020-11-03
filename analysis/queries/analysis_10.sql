USE ${DB};

-------------------------------------------------------------------------
-- Query #10
-- Top 10 Application Failures in Queues with 'pending containers' - DETAILED
-------------------------------------------------------------------------
SELECT
    SUBSTRING(PRTS.REPORTING_TS, 0, 16) AS PENDING_RPT_MN,
    TFA.QUEUE,
    TFA.STARTED_TIME,
    TFA.FINISHED_TIME,
    TFA.APPLICATION_TYPE,
    TFA.ELAPSED_TIME,
    TFA.FINAL_STATUS,
    TFA.ID
FROM
    TOP_ELAPSED_FAILED_APPS TFA
        INNER JOIN
        PENDING_REPORTING_TS PRTS ON
                PRTS.REPORTING_TS >= TFA.STARTED_TIME AND
                PRTS.REPORTING_TS <= TFA.FINISHED_TIME
WHERE
      TFA.REPORTING_TS LIKE "${RPT_DT}%"
  AND TFA.QUEUE != "default"
  AND TFA.RANK < 11
ORDER BY
    TFA.QUEUE, TFA.RANK;


-- PENDING_REPORTING_TS