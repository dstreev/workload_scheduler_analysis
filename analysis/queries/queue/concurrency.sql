USE ${DB};

SHOW CREATE TABLE app;

SELECT
    reporting_ts,
    from_unixtime(CAST(STARTED_TIME / 1000 AS BIGINT))  AS started_time,
    from_unixtime(CAST(FINISHED_TIME / 1000 AS BIGINT)) AS finished_time,
    id,
    user_,
    name,
    queue,
    state,
    final_status,
    progress,
--     tracking_ui,
--     tracking_url,
    diagnostics,
    cluster_id,
    application_type,
    application_tags,
    priority,
--     from_unixtime(CAST(STARTED_TIME / 1000 AS BIGINT))  AS started_time,
    from_unixtime(CAST(launch_time / 1000 AS BIGINT))   AS launch_time,
--     from_unixtime(CAST(FINISHED_TIME / 1000 AS BIGINT)) AS finished_time,
    elapsed_time,
--     am_container_logs,
--     am_host_http_address,
--     am_rpc_address,
--     master_node_id,
--     allocated_mb,
--     allocated_vcores,
--     reserved_vcores,
--     running_containers,
    memory_seconds,
    vcore_seconds,
--     queue_usage_percentage,
--     cluster_usage_percentage,
--     preempted_resource_mb,
--     preempted_resource_vcores,
--     num_non_am_container_preempted,
--     num_am_container_preempted,
    log_aggregation_status,
    unmanaged_application
--     app_node_label_expression,
--     am_node_label_expression
FROM
    app
WHERE
      reporting_ts LIKE "${RPT_DT}%"
--       STARTED_TIME <= unix_timestamp("${RPT_MN}") * 1000
--   AND FINISHED_TIME >= unix_timestamp("${RPT_MN}") * 1000

  AND QUEUE LIKE "${QUEUE}%"
ORDER BY
    started_time;

SELECT
    REPORTING_TS,
    queue_path,
    queue_name,
    capacity,
    used_capacity,
    max_capacity,
    absolute_capacity,
    absolute_max_capacity,
    absolute_used_capacity,
    num_applications,
    allocated_containers,
    reserved_containers,
    pending_containers,
    num_containers,
    max_applications,
    max_applications_per_user,
    user_limit,
    user_num_active_applications,
    user_num_pending_applications,
    user_username AS user_,
--     user_userweight,
--     user_isactive,
    user_resources_used_memory,
    user_resources_used_vcores
FROM
    QUEUE_USAGE
WHERE
      REPORTING_TS LIKE "${RPT_DT}%"
  AND QUEUE_NAME LIKE "${QUEUE}%";


-- Used to tie queue usage and applications together.
SELECT
    QSUB.REPORTING_TS,
    QSUB.QUEUE_PATH,
    QSUB.QUEUE_NAME,
    QSUB.ABSOLUTE_CAPACITY,
    QSUB.ABSOLUTE_USED_CAPACITY,
    QSUB.QU_USERS,
    collect_list(A.ID)    AS APP_IDS,
    collect_list(A.USER_) AS A_USERS,
    COUNT(1)              AS APP_COUNT

FROM
    (
        SELECT
            Q.REPORTING_TS,
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.ABSOLUTE_CAPACITY,
            Q.ABSOLUTE_USED_CAPACITY,
            collect_list(Q.user_username) AS QU_USERS
        FROM
            QUEUE_USAGE Q
        WHERE
              Q.REPORTING_TS LIKE "${RPT_DT}%"
          AND Q.QUEUE_NAME LIKE "${QUEUE}%"
        GROUP BY
            Q.REPORTING_TS,
            Q.QUEUE_PATH,
            Q.QUEUE_NAME,
            Q.ABSOLUTE_CAPACITY,
            Q.ABSOLUTE_USED_CAPACITY
    ) QSUB
        INNER JOIN APP A ON A.QUEUE = QSUB.QUEUE_NAME
        AND A.STARTED_TIME <= unix_timestamp(QSUB.REPORTING_TS) * 1000
        AND A.FINISHED_TIME >= unix_timestamp(QSUB.REPORTING_TS) * 1000
GROUP BY
    QSUB.REPORTING_TS,
    QSUB.QUEUE_PATH,
    QSUB.QUEUE_NAME,
    QSUB.ABSOLUTE_CAPACITY,
    QSUB.ABSOLUTE_USED_CAPACITY,
    QSUB.QU_USERS
ORDER BY
    QSUB.ABSOLUTE_USED_CAPACITY DESC,
    QSUB.REPORTING_TS;



SELECT
    Q.REPORTING_TS,
    Q.QUEUE_PATH,
    Q.QUEUE_NAME,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY,
    collect_list(A.id)    AS APP_IDS,
    collect_list(A.USER_) AS USERS,
    COUNT(1)              AS APP_COUNT
FROM
    QUEUE_USAGE Q
        INNER JOIN APP A ON
            A.QUEUE = Q.QUEUE_NAME
            AND A.STARTED_TIME <= unix_timestamp(Q.REPORTING_TS) * 1000
            AND A.FINISHED_TIME >= unix_timestamp(Q.REPORTING_TS) * 1000
            AND Q.REPORTING_TS LIKE "${RPT_DT}%"
            AND Q.QUEUE_NAME LIKE "${QUEUE}%"
GROUP BY
    Q.REPORTING_TS,
    Q.QUEUE_PATH,
    Q.QUEUE_NAME,
    Q.ABSOLUTE_CAPACITY,
    Q.ABSOLUTE_USED_CAPACITY