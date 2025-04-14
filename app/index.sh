#!/bin/bash


HDFS_INPUT_DIR="/index/data"

if [ "$1" ]; then
    LOCAL_FILE=$1
    hdfs dfs -mkdir -p /tmp/index/input
    hdfs dfs -put $LOCAL_FILE /tmp/index/input
    INPUT_PATH="/tmp/index/input"
else
    INPUT_PATH=$HDFS_INPUT_DIR
fi

OUTPUT_DIR="/tmp/index/output"
hdfs dfs -rm -r $OUTPUT_DIR

source ../venv/bin/activate
PYTHON_PATH=$(which python3)
MAPPER_PATH="$(pwd)/mapreduce/mapper1.py"
REDUCER_PATH="$(pwd)/mapreduce/reducer1.py"

HADOOP_STREAMING_JAR_PATH="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar"
hadoop jar $HADOOP_STREAMING_JAR_PATH \
    -input $INPUT_PATH \
    -output $OUTPUT_DIR \
    -mapper "$PYTHON_PATH $MAPPER_PATH" \
    -reducer "$PYTHON_PATH $REDUCER_PATH"