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
      nohup ./report.sh -DB ${WORKLOAD_DB} -RPT_DT ${1} &
      shift
      ;;
  esac
done
