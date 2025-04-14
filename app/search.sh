#!/bin/bash

export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export YARN_CONF_DIR="$HADOOP_HOME/etc/hadoop"

QUERY="$1"

if [ -z "$QUERY" ]; then
    echo "Please provide a query."
    exit 1
fi

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.cassandra.connection.host=127.0.0.1 \
    --driver-memory 4g \
    --executor-memory 4g \
    query.py "$QUERY"