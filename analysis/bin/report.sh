#!/usr/bin/env bash

cd `dirname $0`

DB=${WORKLOAD_DB:-workload-analysis}
RPT_DT=${REPORTING_DT:-`date +%Y-%m-%d`}
RPT_DIR=${REPORTING_DIR:-${HOME}/workload-analysis/`date +%Y-%m-%d`}

mkdir -p $RPT_DIR

echo "================================================================"
echo "Hive DB:                    ${DB}"
echo "Reporting Date:             ${RPT_DT}"
echo "Reporting Output Directory: ${RPT_DIR}"
echo "================================================================"

echo "Are these the values you want to use?  "
echo "If not, set env variables "
echo "      WORKLOAD_DB, REPORTING_DT, and REPORTING_DIR to override."

echo "Continue? (y)/n"
read cont

RUN_THIS=${cont:-y}

if [ "$RUN_THIS" != "y" ]; then
  echo "Aborted"
  return -1
fi

# Cycle through the Analysis Reports
for i in {1..11}; do
  echo "Running Analysis: ${i}"
  echo "-----------------------------------------------------------------"
  hive --hivevar DB=${DB} --hivevar RPT_DT=${RPT_DT} --silent=false --outputformat=tsv2 --showHeader=true -f analysis/analysis_${i}.sql > ${RPT_DIR}/RPT_${i}.txt
  echo "-----------------------------------------------------------------"
done