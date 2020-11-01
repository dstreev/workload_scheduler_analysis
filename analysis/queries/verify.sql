USE ${DB};

SHOW TABLES;
SHOW CREATE TABLE APP;
SHOW CREATE TABLE QUEUE;
SHOW CREATE TABLE QUEUE_USAGE;

SELECT * FROM HIERARCHY;

SELECT distinct reporting_ts FROM app order by reporting_ts;

SELECT count(*) FROM queue ;

SELECT max(reporting_ts) FROM queue_usage limit 10;

SELECT * FROM app limit 10;
SELECT * FROM queue limit 10;
SELECT * FROM queue_usage limit 10;

SHOW CREATE TABLE queue;

    ALTER TABLE queue_usage SET LOCATION 'hdfs://HOME90/warehouse/tablespace/external/hive/home90_workload.db/queue_usage';

SELECT * FROM APP WHERE REPORTING_TS = "2020-10-20 09:00:05-0400";