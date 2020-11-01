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
    LAUNCH_TIME                    BIGINT,
    FINISHED_TIME                  BIGINT,
    ELAPSED_TIME                   BIGINT,
    AM_CONTAINER_LOGS              STRING,
    AM_HOST_HTTP_ADDRESS           STRING,
    AM_RPC_ADDRESS                 STRING,
    MASTER_NODE_ID                 STRING,
    ALLOCATED_MB                   BIGINT,
    ALLOCATED_VCORES               BIGINT,
    RESERVED_MB                    BIGINT,
    RESERVED_VCORES                BIGINT,
    RUNNING_CONTAINERS             INT,
    MEMORY_SECONDS                 BIGINT,
    VCORE_SECONDS                  BIGINT,
    QUEUE_USAGE_PERCENTAGE         FLOAT,
    CLUSTER_USAGE_PERCENTAGE       FLOAT,
    PREEMPTED_RESOURCE_MB          BIGINT,
    PREEMPTED_RESOURCE_VCORES      BIGINT,
    NUM_NON_AM_CONTAINER_PREEMPTED INT,
    NUM_AM_CONTAINER_PREEMPTED     INT,
    LOG_AGGREGATION_STATUS         STRING,
    UNMANAGED_APPLICATION          STRING,
    APP_NODE_LABEL_EXPRESSION      STRING,
    AM_NODE_LABEL_EXPRESSION       STRING
);

