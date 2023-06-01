show databases;
DROP DATABASE IF EXISTS ${DB}_source CASCADE;
-- EXTERNAL LANDING LOCATION
CREATE DATABASE IF NOT EXISTS ${DB}_source;
-- MANAGED DB
DROP DATABASE IF EXISTS ${DB} CASCADE;
CREATE DATABASE IF NOT EXISTS ${DB};

-- NOTE: Where you see table names in lower case, this is important because
--       it matches the names of the folders populated by the load process
--       which is lowercase

USE ${DB}_source;


-- DROP TABLE IF EXISTS hierarchy;
--
-- CREATE EXTERNAL TABLE IF NOT EXISTS hierarchy (
--                                                   PARENT STRING,
--                                                   CHILD STRING,
--                                                   CAPACITY DOUBLE,
--                                                   MAX_CAPACITY DOUBLE
-- );

DROP TABLE IF EXISTS queue;
CREATE EXTERNAL TABLE IF NOT EXISTS queue
(
    REPORTING_TS                     STRING,
    QUEUE_PATH                       STRING,
    TYPE                             STRING,
    CAPACITY                         FLOAT,
    USED_CAPACITY                    FLOAT,
    MAX_CAPACITY                     FLOAT,
    ABSOLUTE_CAPACITY                FLOAT,
    ABSOLUTE_MAX_CAPACITY            FLOAT,
    ABSOLUTE_USED_CAPACITY           FLOAT,
    NUM_APPLICATIONS                 INT,
    USED_RESOUCES                    BIGINT,
    QUEUE_NAME                       STRING,
    STATE                            STRING,
    RESOURCES_USED_MEMORY            BIGINT,
    RESOURCES_USED_VCORES            BIGINT,
    HIDE_RESERVATION_QUEUES          STRING,
    ALLOCATED_CONTAINERS             BIGINT,
    RESERVED_CONTAINERS              BIGINT,
    PENDING_CONTAINERS               BIGINT,
    MIN_EFFECTIVE_CAPACITY_MEMORY    BIGINT,
    MIN_EFFECTIVE_CAPACITY_VCORES    BIGINT,
    MAX_EFFECTIVE_CAPACITY_MEMORY    BIGINT,
    MAX_EFFECTIVE_CAPACITY_VCORES    BIGINT,
    MAXIMUM_ALLOCATION_MEMORY        BIGINT,
    MAXIMUM_ALLOCATION_VCORES        BIGINT,
    QUEUE_PRIORITY                   STRING,
    ORDERING_POLICY_INFO             STRING,
    AUTO_CREATE_CHILD_QUEUE_ENABLED  STRING,
    NUM_ACTIVE_APPLICATIONS          INT,
    NUM_PENDING_APPLICATIONS         INT,
    NUM_CONTAINERS                   INT,
    MAX_APPLICATIONS                 INT,
    MAX_APPLICATIONS_PER_USER        INT,
    USER_LIMIT                       INT,
    USER_LIMIT_FACTOR                FLOAT,
    CONFIGURED_MAX_AM_RESOURCE_LIMIT BIGINT,
    AM_RESOURCE_LIMIT_MEMORY         BIGINT,
    AM_RESOURCE_LIMIT_VCORES         BIGINT,
    USED_AM_RESOURCE_MEMORY          BIGINT,
    USED_AM_RESOURCE_VCORES          BIGINT,
    USER_AM_RESOURCE_LIMIT_MEMORY    BIGINT,
    USER_AM_RESOURCE_LIMIT_VCORES    BIGINT,
    PREEMPTION_DISABLED              STRING,
    INTRA_QUEUE_PREEMPTION_DISABLED  STRING,
    DEFAULT_PRIORITY                 STRING,
    IS_AUTO_CREATED_LEAF_QUEUE       STRING,
    MAX_APPLICATION_LIFETIME         STRING,
    DEFAULT_APPLICATION_LIFETIME     STRING
)
PARTITIONED BY (
    -- load_hour format should be 'yyyy-MM-dd_HH24'
    load_hour string
    );

DROP TABLE IF EXISTS queue_usage;

CREATE EXTERNAL TABLE IF NOT EXISTS queue_usage
(
    REPORTING_TS                    STRING,
    QUEUE_PATH                      STRING,
    CAPACITY                        FLOAT,
    USED_CAPACITY                   FLOAT,
    MAX_CAPACITY                    FLOAT,
    ABSOLUTE_CAPACITY               FLOAT,
    ABSOLUTE_MAX_CAPACITY           FLOAT,
    ABSOLUTE_USED_CAPACITY          FLOAT,
    NUM_APPLICATIONS                INT,
    QUEUE_NAME                      STRING,
    STATE                           STRING,
    HIDE_RESERVATION_QUEUES         STRING,
    ALLOCATED_CONTAINERS            INT,
    RESERVED_CONTAINERS             INT,
    PENDING_CONTAINERS              INT,
    QUEUE_PRIORITY                  STRING,
    ORDERING_POLICY_INFO            STRING,
    AUTO_CREATE_CHILD_QUEUE_ENABLED STRING,
    NUM_ACTIVE_APPLICATIONS         INT,
    NUM_PENDING_APPLICATIONS        INT,
    NUM_CONTAINERS                  INT,
    MAX_APPLICATIONS                INT,
    MAX_APPLICATIONS_PER_USER       INT,
    USER_LIMIT                      INT,
    USER_NUM_ACTIVE_APPLICATIONS    INT,
    USER_NUM_PENDING_APPLICATIONS   INT,
    USER_USERNAME                   STRING,
    USER_USERWEIGHT                 BIGINT,
    USER_ISACTIVE                   STRING,
    USER_RESOURCES_USED_MEMORY      BIGINT,
    USER_RESOURCES_USED_VCORES      BIGINT
)
PARTITIONED BY (
    -- load_hour format should be 'yyyy-MM-dd_HH24'
    load_hour string
    );

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
    STARTED_TIME                   BIGINT,
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
)
PARTITIONED BY (
    -- load_hour format should be 'yyyy-MM-dd_HH24'
    load_hour string
    );


USE ${DB};

-- Create Managed Version
-- SKIPPING the LIKE.  I need to change the PARTITIONING.
-- DROP TABLE IF EXISTS app;
-- CREATE TABLE app LIKE ${DB}_source.app
-- STORED AS ORC
-- TBLPROPERTIES ('transactional'='true','transactional_properties'='INSERT_ONLY');
-- -- Change the Location to the Managed Warehouse
-- -- Bug Filed for this: https://jira.cloudera.com/browse/CDPD-18875 (Still exists in CDP 7.1.8)
-- ALTER TABLE app SET LOCATION "/warehouse/tablespace/managed/hive/${DB}.db/app";
--
-- DROP TABLE IF EXISTS queue;
-- CREATE TABLE queue LIKE ${DB}_source.queue
-- STORED AS ORC
-- TBLPROPERTIES ('transactional'='true','transactional_properties'='INSERT_ONLY');
-- -- Change the Location to the Managed Warehouse
-- -- Bug Filed for this: https://jira.cloudera.com/browse/CDPD-18875 (Still exists in CDP 7.1.8)
-- ALTER TABLE queue SET LOCATION "/warehouse/tablespace/managed/hive/${DB}.db/queue";
--
--
-- DROP TABLE IF EXISTS queue_usage;
-- CREATE TABLE queue_usage LIKE ${DB}_source.queue_usage
-- STORED AS ORC
-- TBLPROPERTIES ('transactional'='true','transactional_properties'='INSERT_ONLY');
-- -- Change the Location to the Managed Warehouse
-- -- Bug Filed for this: https://jira.cloudera.com/browse/CDPD-18875 (Still exists in CDP 7.1.8)
-- ALTER TABLE queue_usage SET LOCATION "/warehouse/tablespace/managed/hive/${DB}.db/queue_usage";

-- NOTE: Removed partitions for queue because there's not enough data to justify over 1 year period.
DROP TABLE IF EXISTS queue;
CREATE TABLE IF NOT EXISTS queue
(
    REPORTING_TS                     STRING,
    QUEUE_PATH                       STRING,
    TYPE                             STRING,
    CAPACITY                         FLOAT,
    USED_CAPACITY                    FLOAT,
    MAX_CAPACITY                     FLOAT,
    ABSOLUTE_CAPACITY                FLOAT,
    ABSOLUTE_MAX_CAPACITY            FLOAT,
    ABSOLUTE_USED_CAPACITY           FLOAT,
    NUM_APPLICATIONS                 INT,
    USED_RESOUCES                    BIGINT,
    QUEUE_NAME                       STRING,
    STATE                            STRING,
    RESOURCES_USED_MEMORY            BIGINT,
    RESOURCES_USED_VCORES            BIGINT,
    HIDE_RESERVATION_QUEUES          STRING,
    ALLOCATED_CONTAINERS             BIGINT,
    RESERVED_CONTAINERS              BIGINT,
    PENDING_CONTAINERS               BIGINT,
    MIN_EFFECTIVE_CAPACITY_MEMORY    BIGINT,
    MIN_EFFECTIVE_CAPACITY_VCORES    BIGINT,
    MAX_EFFECTIVE_CAPACITY_MEMORY    BIGINT,
    MAX_EFFECTIVE_CAPACITY_VCORES    BIGINT,
    MAXIMUM_ALLOCATION_MEMORY        BIGINT,
    MAXIMUM_ALLOCATION_VCORES        BIGINT,
    QUEUE_PRIORITY                   STRING,
    ORDERING_POLICY_INFO             STRING,
    AUTO_CREATE_CHILD_QUEUE_ENABLED  STRING,
    NUM_ACTIVE_APPLICATIONS          INT,
    NUM_PENDING_APPLICATIONS         INT,
    NUM_CONTAINERS                   INT,
    MAX_APPLICATIONS                 INT,
    MAX_APPLICATIONS_PER_USER        INT,
    USER_LIMIT                       INT,
    USER_LIMIT_FACTOR                FLOAT,
    CONFIGURED_MAX_AM_RESOURCE_LIMIT BIGINT,
    AM_RESOURCE_LIMIT_MEMORY         BIGINT,
    AM_RESOURCE_LIMIT_VCORES         BIGINT,
    USED_AM_RESOURCE_MEMORY          BIGINT,
    USED_AM_RESOURCE_VCORES          BIGINT,
    USER_AM_RESOURCE_LIMIT_MEMORY    BIGINT,
    USER_AM_RESOURCE_LIMIT_VCORES    BIGINT,
    PREEMPTION_DISABLED              STRING,
    INTRA_QUEUE_PREEMPTION_DISABLED  STRING,
    DEFAULT_PRIORITY                 STRING,
    IS_AUTO_CREATED_LEAF_QUEUE       STRING,
    MAX_APPLICATION_LIFETIME         STRING,
    DEFAULT_APPLICATION_LIFETIME     STRING
)
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

DROP TABLE IF EXISTS queue_usage;

CREATE TABLE IF NOT EXISTS queue_usage
(
    REPORTING_TS                    STRING,
    QUEUE_PATH                      STRING,
    CAPACITY                        FLOAT,
    USED_CAPACITY                   FLOAT,
    MAX_CAPACITY                    FLOAT,
    ABSOLUTE_CAPACITY               FLOAT,
    ABSOLUTE_MAX_CAPACITY           FLOAT,
    ABSOLUTE_USED_CAPACITY          FLOAT,
    NUM_APPLICATIONS                INT,
    QUEUE_NAME                      STRING,
    STATE                           STRING,
    HIDE_RESERVATION_QUEUES         STRING,
    ALLOCATED_CONTAINERS            INT,
    RESERVED_CONTAINERS             INT,
    PENDING_CONTAINERS              INT,
    QUEUE_PRIORITY                  STRING,
    ORDERING_POLICY_INFO            STRING,
    AUTO_CREATE_CHILD_QUEUE_ENABLED STRING,
    NUM_ACTIVE_APPLICATIONS         INT,
    NUM_PENDING_APPLICATIONS        INT,
    NUM_CONTAINERS                  INT,
    MAX_APPLICATIONS                INT,
    MAX_APPLICATIONS_PER_USER       INT,
    USER_LIMIT                      INT,
    USER_NUM_ACTIVE_APPLICATIONS    INT,
    USER_NUM_PENDING_APPLICATIONS   INT,
    USER_USERNAME                   STRING,
    USER_USERWEIGHT                 BIGINT,
    USER_ISACTIVE                   STRING,
    USER_RESOURCES_USED_MEMORY      BIGINT,
    USER_RESOURCES_USED_VCORES      BIGINT
)
PARTITIONED BY (
    -- load_month format should be 'yyyy-MM'
    load_month string
)
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

DROP TABLE IF EXISTS app;

CREATE TABLE IF NOT EXISTS app
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
    STARTED_TIME                   BIGINT,
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
)
PARTITIONED BY (
    -- load_month format should be 'yyyy-MM'
    load_month string
)
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
