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
      nohup ./report.sh ${WORKLOAD_DB} ${1} &
      shift
      ;;
  esac
done
