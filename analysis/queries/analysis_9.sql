USE ${DB};

-------------------------------------------------------------------------
-- Query #9
-- Top 10 Application Failures in Queues with 'pending containers' - SUMMARY
-------------------------------------------------------------------------
SELECT
    TFA.QUEUE,
    TFA.ID,
    TFA.APPLICATION_TYPE,
    TFA.STARTED_TIME,
    TFA.FINISHED_TIME,
    TFA.FINAL_STATUS,
    TFA.ELAPSED_TIME,
    COUNT(1) NUM_OF_TIMES_WITH_PENDING
FROM
    TOP_ELAPSED_FAILED_APPS TFA
        INNER JOIN
        PENDING_REPORTING_TS PRTS ON
                PRTS.REPORTING_TS >= TFA.STARTED_TIME AND
                PRTS.REPORTING_TS <= TFA.FINISHED_TIME
WHERE
      TFA.REPORTING_TS LIKE "${RPT_DT}%"
  AND TFA.RANK < 11
GROUP BY
    TFA.QUEUE,
    TFA.ID,
    TFA.APPLICATION_TYPE,
    TFA.STARTED_TIME,
    TFA.FINISHED_TIME,
    TFA.FINAL_STATUS,
    TFA.ELAPSED_TIME
ORDER BY
    TFA.QUEUE, TFA.ELAPSED_TIME;


-- PENDING_REPORTING_TS