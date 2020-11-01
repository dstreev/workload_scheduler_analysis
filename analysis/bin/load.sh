#!/usr/bin/env bash

cd `dirname $0`

DB=${WORKLOAD_DB:-workload-analysis}

# Build out the Schema
hive --hivevar DB=${DB} -f transform/analysis_transform.sql