#!/usr/bin/env bash

cd $(dirname $0)

while [[ $# -gt 0 ]]; do
  case "$1" in
    -DB)
      shift
      WORKLOAD_DB=${1}
      shift
      ;;
    *)
      if [[ "${WORKLOAD_DB}x" == "x" ]]; then
        echo "DB not set"
        echo "Usage: reports.sh -DB <db> <report_date_yyyy-MM-dd> [<report_date_yyyy-MM-dd>]..."
        exit -1
      fi
      nohup ./report.sh -DB ${WORKLOAD_DB} -RPT_DT ${1} &
      shift
      ;;
  esac
done
