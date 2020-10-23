USE ${DB};

DROP TABLE IF EXISTS app;

CREATE EXTERNAL TABLE IF NOT EXISTS app
(
    REPORTING_TS                   STRING,
    ID                             STRING,
    USER_                          STRING,
    NAME                           STRING,
    QUEUE                          STRING,
    STATE                          STRING,
    FINAL_STATUS                   STRING,
    PROGRESS                       STRING,
    TRACKING_UI                    STRING,
    TRACKING_URL                   STRING,
    DIAGNOSTICS                    STRING,
    CLUSTER_ID                     STRING,
    APPLICATION_TYPE               STRING,
    APPLICATION_TAGS               STRING,
    PRIORITY                       STRING,
    STARTED_TIME                   STRING,
    LAUNCH_TIME                    STRING,
    FINISHED_TIME                  STRING,
    ELAPSED_TIME                   STRING,
    AM_CONTAINER_LOGS              STRING,
    AM_HOST_HTTP_ADDRESS           STRING,
    AM_RPC_ADDRESS                 STRING,
    MASTER_NODE_ID                 STRING,
    ALLOCATED_MB                   STRING,
    ALLOCATED_VCORES               STRING,
    RESERVED_MB                    STRING,
    RESERVED_VCORES                STRING,
    RUNNING_CONTAINERS             STRING,
    MEMORY_SECONDS                 STRING,
    VCORE_SECONDS                  STRING,
    QUEUE_USAGE_PERCENTAGE         STRING,
    CLUSTER_USAGE_PERCENTAGE       STRING,
    PREEMPTED_RESOURCE_MB          STRING,
    PREEMPTED_RESOURCE_VCORES      STRING,
    NUM_NON_AM_CONTAINER_PREEMPTED STRING,
    NUM_AM_CONTAINER_PREEMPTED     STRING,
    LOG_AGGREGATION_STATUS         STRING,
    UNMANAGED_APPLICATION          STRING,
    APP_NODE_LABEL_EXPRESSION      STRING,
    AM_NODE_LABEL_EXPRESSION       STRING
);

