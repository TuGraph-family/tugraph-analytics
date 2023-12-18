#!/usr/bin/env bash
#
# Copyright 2023 AntGroup CO., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

if [ -z "$JAVA_HOME" ] ; then
  JAVACMD=`which java`
else
  JAVACMD="$JAVA_HOME/bin/java"
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "The JAVA_HOME environment variable is not defined correctly" >&2
  exit 1
fi

DIR=`dirname "$0"`
DIR=`cd "$DIR"; pwd`

# build classpath
GEAFLOW_JAR_DIR=${DIR}/../geaflow/geaflow-deploy/geaflow-assembly/target/

echo $GEAFLOW_JAR_DIR
FAT_JAR=`ls $GEAFLOW_JAR_DIR | grep "^geaflow-assembly-.*[^sources].jar"`
CLASSPATH="$GEAFLOW_JAR_DIR/$FAT_JAR"

TMP_DIR=/tmp/geaflow
GEAFLOW_LOG_DIR=${TMP_DIR}/logs
GEAFLOW_LOG_PATH=$GEAFLOW_LOG_DIR/local.log
mkdir -p $GEAFLOW_LOG_DIR

LOG4j_PROPERTIES_FILE_NAME=log4j.properties

while [ "$1" ]; do
  case "$1" in
  --gql)
    GQL_FILE="$2"
    shift
    ;;
  --profiler)
    ASYNC_PROFILER_SHELL_PATH="$2"
    shift
    ;;
  --args)
    JOB_ARGS="$2"
    shift
    ;;
  esac
  shift
  done

DEFAULT_JOB_ARGS='{"job": {"geaflow.log.dir": "/tmp/geaflow/logs", "geaflow.agent.http.port": "8088", "AGENT_PROFILER_PATH": "'${ASYNC_PROFILER_SHELL_PATH}'"}}'
JOB_ARGS=${JOB_ARGS:-${DEFAULT_JOB_ARGS}}
echo "JOB_ARGS:  ${JOB_ARGS}"

echo "GQL_FILE: $GQL_FILE"
mkdir -p /tmp/geaflow/gql
cat $GQL_FILE > /tmp/geaflow/gql/user.gql

CLASSPATH=$CLASSPATH:/tmp/geaflow/gql/

echo "CLASSPATH:$CLASSPATH"
echo -e "\033[32mView dashboard via http://localhost:8090.
See logs via url http://localhost:8090/#/components/master/logs or at local path ${GEAFLOW_LOG_PATH}\033[32m"
$JAVACMD -cp "$CLASSPATH" \
  -DclusterType=LOCAL \
  -Dlog.file=${GEAFLOW_LOG_PATH} \
  -Dlog4j.configuration=${LOG4j_PROPERTIES_FILE_NAME} \
  com.antgroup.geaflow.dsl.runtime.engine.GeaFlowGqlClient "${JOB_ARGS}" > ${GEAFLOW_LOG_PATH} 2>&1