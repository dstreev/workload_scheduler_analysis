#!/usr/bin/env bash

cd $(dirname $0)

while [[ $# -gt 0 ]]; do
  case "$1" in
    *)
      nohup ./report.sh ${1} &
      shift
      ;;
  esac
done