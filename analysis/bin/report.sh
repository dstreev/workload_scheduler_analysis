#!/usr/bin/env bash

cd $(dirname $0)

DB=${WORKLOAD_DB:-workload-analysis}
RPT_DT=${REPORTING_DT:-$(date +%Y-%m-%d)}
RPT_DIR=${REPORTING_DIR:-${HOME}/workload-analysis/${DB}/${RPT_DT}}

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
  exit 1
fi

# Cycle through the Analysis Reports
for i in {1..13}; do
  if [ -f ../queries/analysis_${i}.sql ]; then
    echo "Running Analysis: ${i}"
    echo "-----------------------------------------------------------------"
    hive --hivevar DB=${DB} --hivevar RPT_DT=${RPT_DT} --silent=false --outputformat=dsv --showHeader=true -f ../queries/analysis_${i}.sql >${RPT_DIR}/ANALYSIS_RPT_${i}.txt
    echo "-----------------------------------------------------------------"
    ./toMD.sh ${RPT_DIR}/ANALYSIS_RPT_${i}.txt
  fi
done

if [ -f ${RPT_DIR}/REPORT.md ]; then
  rm ${RPT_DIR}/REPORT.md
fi

## Build Report
for i in {1..13}; do
  if [ -f ${RPT_DIR}/ANALYSIS_RPT_${i}.txt.md ]; then
    cat ../queries/analysis_${i}.md >> ${RPT_DIR}/REPORT.md
    cat ${RPT_DIR}/ANALYSIS_RPT_${i}.txt.md >> ${RPT_DIR}/REPORT.md
    echo " " >> ${RPT_DIR}/REPORT.md
  fi
done