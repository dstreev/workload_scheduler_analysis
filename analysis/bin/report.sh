#!/usr/bin/env bash

cd $(dirname $0)

while [[ $# -gt 0 ]]; do
  case "$1" in
    -DB|--DATABASE)
      shift
      WORKLOAD_DB=${1}
      shift
      ;;
    -RPT_DT|--REPORT_DATE)
      shift
      REPORTING_DT=${1}
      shift
      ;;
  esac
done

DB=${WORKLOAD_DB:-workload-analysis}
RPT_DT=${REPORTING_DT:-$(date +%Y-%m-%d)}
RPT_DIR=${REPORTING_DIR:-${HOME}/workload-analysis/${DB}/${RPT_DT}}

RPT_FILE=REPORT_${RPT_DT}.md
RPT_DTL_FILE=REPORT_DTL_${RPT_DT}.md

FULL_RPT_FILE=${RPT_DIR}/${RPT_FILE}
FULL_RPT_DTL_FILE=${RPT_DIR}/${RPT_DTL_FILE}

# Need this to build detail toc in main file.
# Only works when the output md's are converted to html.
RPT_DTL_FILE_HTML="${RPT_DTL_FILE:0:$((${#RPT_DTL_FILE}-2))}md"
#echo "*******************************************"
#echo "$RPT_DTL_FILE_HTML"
#echo "*******************************************"

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

#echo "Are these the values you want to use?  "
#echo "If not, set env variables "
#echo "      WORKLOAD_DB, REPORTING_DT, and REPORTING_DIR to override."

#echo "Continue? (y)/n"
#read cont

#RUN_THIS=${cont:-y}

#if [ "$RUN_THIS" != "y" ]; then
#  echo "Aborted"
#  exit 1
#fi

# Generate Header TODO
# Add report variables to report output.

echo "Running Report Range Query"
echo "-----------------------------------------------------------------"
hive --hivevar DB=${DB} --hivevar RPT_DT=${RPT_DT} --silent=false --outputformat=dsv --showHeader=true -f ../queries/range.sql >${RPT_DIR}/RPT_RANGE.txt
echo "-----------------------------------------------------------------"
./toMD.sh ${RPT_DIR}/RPT_RANGE.txt


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

# Report Header
cat ../queries/header.md > ${FULL_RPT_FILE}

# Report Range Details
cat ../queries/range.md >> ${FULL_RPT_FILE}
echo " " >> ${FULL_RPT_FILE}
echo "Hive DB:                    ${DB}" >> ${FULL_RPT_FILE}
echo "Reporting Date:             ${RPT_DT}" >> ${FULL_RPT_FILE}
echo " " >> ${FULL_RPT_FILE}
cat ${RPT_DIR}/RPT_RANGE.txt.md >> ${FULL_RPT_FILE}

echo " " >> ${FULL_RPT_FILE}
echo "## Table of Contents" >> ${FULL_RPT_FILE}
echo " " >> ${FULL_RPT_FILE}
echo "<!--TOC-->" >> ${FULL_RPT_FILE}
#echo "<!--te-->" >> ${FULL_RPT_FILE}
echo " " >> ${FULL_RPT_FILE}

# Build the Detailed TOC
echo "## Detailed Reports Table of Contents" >> ${FULL_RPT_FILE}
echo " " >> ${FULL_RPT_FILE}

#echo "* [Lost Opportunities](./${RPT_DTL_FILE_HTML}/#lost-opportunities-detailed)" >> ${FULL_RPT_FILE}
echo "* <a href=\"./${RPT_DTL_FILE_HTML}/#lostopportunitiesdetailed\" target=\"_blank\">Lost Opportunities</a>" >> ${FULL_RPT_FILE}
#echo "* [Cluster Used Metrics Detailed](./${RPT_DTL_FILE_HTML}/#cluster-used-metrics-detailed)" >> ${FULL_RPT_FILE}
echo "* <a href=\"./${RPT_DTL_FILE_HTML}/#clusterusedmetricsdetailed\" target=\"_blank\">Cluster Used Metrics Detailed</a>" >> ${FULL_RPT_FILE}
#echo "* [Top Application Failures](./${RPT_DTL_FILE_HTML}/#top-application-failures-detailed)" >> ${FULL_RPT_FILE}
echo "* <a href=\"./${RPT_DTL_FILE_HTML}/#topapplicationfailuresdetailed\" target=\"_blank\">Top Application Failures</a>" >> ${FULL_RPT_FILE}
#echo "* [Queue User Count](./${RPT_DTL_FILE_HTML}/#queue-user-count-detailed)" >> ${FULL_RPT_FILE}
echo "* <a href=\"./${RPT_DTL_FILE_HTML}/#queueusercountdetailed\" target=\"_blank\">Queue User Count</a>" >> ${FULL_RPT_FILE}
echo " " >> ${FULL_RPT_FILE}
echo "## Analysis Summaries" >> ${FULL_RPT_FILE}

## Build Report
for i in {1..99}; do
  if [ -f ${RPT_DIR}/ANALYSIS_RPT_${i}.txt.md ]; then
    cat ../queries/analysis_${i}.md >> ${FULL_RPT_FILE}
    cat ${RPT_DIR}/ANALYSIS_RPT_${i}.txt.md >> ${FULL_RPT_FILE}
    cat ../queries/toc_ref.md >> ${FULL_RPT_FILE}
  fi
done

## Build Report Details
cat ../queries/detailed/detailed-header.md > ${FULL_RPT_DTL_FILE}
for i in {1..99}; do
  if [ -f ${RPT_DIR}/ANALYSIS_DTL_RPT_${i}.txt.md ]; then
    cat ../queries/detailed/analysis_${i}.md >> ${FULL_RPT_DTL_FILE}
    cat ${RPT_DIR}/ANALYSIS_DTL_RPT_${i}.txt.md >> ${FULL_RPT_DTL_FILE}

    cat ../queries/toc_ref.md >> ${FULL_RPT_DTL_FILE}
  fi
done

## Build Report Appendix
cat ../report_appendix/report_appendix.md >> ${FULL_RPT_FILE}
for file in `ls ../report_appendix/appendix_*.md`; do
  cat $file >> ${FULL_RPT_FILE}
  cat ../queries/toc_ref.md >> ${FULL_RPT_FILE}
done

## Build TOC for Report
gh-md-toc --insert ${FULL_RPT_FILE}
