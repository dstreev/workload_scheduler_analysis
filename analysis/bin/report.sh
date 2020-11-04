#!/usr/bin/env bash

cd $(dirname $0)

REPORTING_DT=$1

DB=${WORKLOAD_DB:-workload-analysis}
RPT_DT=${REPORTING_DT:-$(date +%Y-%m-%d)}
RPT_DIR=${REPORTING_DIR:-${HOME}/workload-analysis/${DB}/${RPT_DT}}

if [ -d ${RPT_DIR} ]; then
  # Delete Previous Results
  rm -r -f ${RPT_DIR}
fi

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

# Generate Header TODO
# Add report variables to report output.

# Cycle through the Analysis Reports
for i in {1..99}; do
  if [ -f ../queries/analysis_${i}.sql ]; then
    echo "Running Analysis: ${i}"
    echo "-----------------------------------------------------------------"
    hive --hivevar DB=${DB} --hivevar RPT_DT=${RPT_DT} --silent=false --outputformat=dsv --showHeader=true -f ../queries/analysis_${i}.sql >${RPT_DIR}/ANALYSIS_RPT_${i}.txt
    echo "-----------------------------------------------------------------"
    ./toMD.sh ${RPT_DIR}/ANALYSIS_RPT_${i}.txt
  fi
done

for i in {1..99}; do
  if [ -f ../queries/detailed/analysis_${i}.sql ]; then
    echo "Running Detailed Analysis: ${i}"
    echo "-----------------------------------------------------------------"
    hive --hivevar DB=${DB} --hivevar RPT_DT=${RPT_DT} --silent=false --outputformat=dsv --showHeader=true -f ../queries/detailed/analysis_${i}.sql >${RPT_DIR}/ANALYSIS_DTL_RPT_${i}.txt
    echo "-----------------------------------------------------------------"
    ./toMD.sh ${RPT_DIR}/ANALYSIS_DTL_RPT_${i}.txt
  fi
done

#if [ -f ${RPT_DIR}/REPORT.md ]; then
#  rm ${RPT_DIR}/REPORT.md
#fi
# Report Header
cat ../queries/header.md > ${RPT_DIR}/REPORT.md

## Build Report
for i in {1..99}; do
  if [ -f ${RPT_DIR}/ANALYSIS_RPT_${i}.txt.md ]; then
    cat ../queries/analysis_${i}.md >> ${RPT_DIR}/REPORT.md
    cat ${RPT_DIR}/ANALYSIS_RPT_${i}.txt.md >> ${RPT_DIR}/REPORT.md
    echo " " >> ${RPT_DIR}/REPORT.md
  fi
done

## Build Report Details
cat ../queries/detailed/detailed-header.md >> ${RPT_DIR}/REPORT.md
for i in {1..99}; do
  if [ -f ${RPT_DIR}/ANALYSIS_DTL_RPT_${i}.txt.md ]; then
    cat ../queries/detailed/analysis_${i}.md >> ${RPT_DIR}/REPORT.md
    cat ${RPT_DIR}/ANALYSIS_DTL_RPT_${i}.txt.md >> ${RPT_DIR}/REPORT.md
    echo " " >> ${RPT_DIR}/REPORT.md
  fi
done

## Build Report Appendix
cat ../report_appendix/report_appendix.md >> ${RPT_DIR}/REPORT.md
for file in `ls ../report_appendix/appendix_*.md`; do
  cat $file >> ${RPT_DIR}/REPORT.md
  echo " " >> ${RPT_DIR}/REPORT.md
done

## Build TOC for Report
markdown-toc -h 3 -t github ${RPT_DIR}/REPORT.md
