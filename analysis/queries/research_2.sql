USE ${DB};

---------------------------------------------------------------
-- Research Query: #2
-- PENDING_QUEUES RAW
---------------------------------------------------------------
SELECT *
FROM
    PENDING_QUEUES PQ
WHERE
      PQ.REPORTING_TS LIKE "${RPT_DT}%"
  AND PQ.QUEUE_NAME LIKE "${QUEUE}"
ORDER BY
    -- Bring the most egregious to the top
    PENDING_CONTAINERS DESC;

