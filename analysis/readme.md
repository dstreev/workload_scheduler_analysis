# Workload Analysis

## Summary

The Workload Analysis process collects data points from YARN containers and scheduler.  With these data points we can reflect upon historical usage patterns that can help us understand where how effective the current queue strategy is.

## Setup

### Hive

Run from an Edge Node. Set the following environment variables for your implementation:
```
export WORKLOAD_DB=<hive db to create/use>
export REPORTING_DIR=<local output directoy for reports>
```

If these aren't set, defaults will be used:

```
DB=${WORKLOAD_DB:-workload-analysis}
RPT_DIR=${REPORTING_DIR:-${HOME}/workload-analysis/${DB}/`date +%Y-%m-%d`}
```

- The Hive [Setup Scripts](bin/setup.sh) used to build the databases and views.
- The Hive [Load Scripts](bin/load.sh) used to transform the source 'external' tables to the 'managed' tables.
- Cycle through each of the [Analysis Reports-Multi-Day](bin/reports.sh) or [Analysis Report-Single-Day](bin/report.shj) and save output to the local filesystem.  These scripts can be run from your favorite JDBC client, like `hive/beeline` or `dbvisualizer`.

### Collector

The Hive SQL scripts runs against the output saved from either of these:
 - [Hive SRE (cli)](https://github.com/dstreev/cloudera_upgrade_utils/blob/master/hive-sre/README.md)
 - [Hadoop CLI](https://github.com/dstreev/hadoop-cli/blob/master/README.md)
 
Install either of these on an edge node to continue, preferably the [Hive SRE (cli)](https://github.com/dstreev/cloudera_upgrade_utils/blob/master/hive-sre/README.md) since this has many other helpful *Hive* tools.

The application has two options we'll use to source data for this process.
- [YARN Container Statistics](https://github.com/dstreev/hadoop-cli/blob/master/README.md#container-stats)
- [YARN Queue Statistics](https://github.com/dstreev/hadoop-cli/blob/master/README.md#scheduler-stats)

Using the `sstat` and `cstat` options above will generate the data required by this process.  `sstat` collects the *current* state of the queues (point in time).  `cstat` will collect `FINISHED` application containers.

#### `crontab` entries

The below entries expect to be on an *edge/gateway* node in the cluster.  Configuratons for the 'hdfs` and `yarn` should be in `/etc/hadoop/conf`.  If you are running a kerberized cluster, a kerberos ticket is required.  The principal of the user will determine the visibility of the `yarn` container stats.  If you aren't seeing ALL the containers, use an account that is a YARN admin.

The location of the *resource manager* is determined by the entries in `yarn-site.xml`.  HA is supported. If you are running TLS, use the `-ssl` option to choose TLS.  If you have a self-sign cert, use `JAVA_OPTS` to set the `truststore` and `password` (see example below).
 
 Use the `-o` to specify an output directory. All output is written/appended to files in HDFS.  The filename will be `yyyy-MM.txt`. 

```
# Running the Scheduler Stats every minute
* * * * * export JAVA_OPTS=-Djavax.net.ssl.trustStore=/home/dstreev/certs/gateway-client-trust.jks -Djavax.net.ssl.trustStorePassword=changeit;/usr/local/bin/hive-sre-cli -s -e "sstat -ssl -o /warehouse/tablespace/external/hive/home90_workload.db"
# Run once a day.  Default will go back 1 day and incrementally build 1 hours segments and run the queries.  So run this at midnight
# to capture the previous days data.
0 * * * * export JAVA_OPTS=-Djavax.net.ssl.trustStore=/home/dstreev/certs/gateway-client-trust.jks -Djavax.net.ssl.trustStorePassword=changeit;/usr/local/bin/hive-sre-cli -s -e "cstat -ssl -l 1-hour -o /warehouse/tablespace/external/hive/home90_workload.db"
```

##### The Run Cycles

`cstat` by default (with no additional parameters), will extract `FINISHED` containers over the last 24 hours.  If the cluster is very busy or you need the result more quickly, use the `-l` option to run for the "last" n (hours|days).  For example: Run the process every hour in `crontab` and use the `-l 1-hour` argument.  Don't overlap timeframes or you will get duplicates.

`sstat` needs to be run every minute in `crontab`

##### Output Directory

The `-o` option is an HDFS location.  When prefixed with `/`, it is an absolute location otherwise it is relative to the principal/users hdfs *home* directory.  If the user does NOT have a home directory in *hdfs* the process will fail.

In the output directory, three sub-directories will be created/used: `app, queue, queue_usage`.  When the location is set to the expected *source* database location in hive's external warehouse directory, your table configuration will pick up the values automatically.

## Reporting

The reports are all Markdown.

### Tooling

[Markdown TOC Creator](https://github.com/ekalinin/github-markdown-toc#auto-insert-and-update-toc)

