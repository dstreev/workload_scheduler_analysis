#!/usr/bin/env bash

cd `dirname $0`

DB=${WORKLOAD_DB:-workload-analysis}
RPT_DT=${REPORTING_DT:-`date +%Y-%m-%d`}
RPT_DIR=${REPORTING_DIR:-${HOME}/workload-analysis/`date +%Y-%m-%d`}

mkdir -p $RPT_DIR

# Cycle through the Analysis Reports
for i in {1..10}; do
  hive --hivevar DB=${DB} --hivevar RPT_DT=${RPT_DT} --silent=false --outputformat=tsv2 --showHeader=true -f analysis/analysis_${i}.sql > ${RPT_DIR}/RPT_${i}.txt
done