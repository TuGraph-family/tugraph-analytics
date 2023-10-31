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


set -e

bin_dir=`dirname "$0"`
bin_dir=`cd "$bin_dir"; pwd`
# get common config
. "$bin_dir"/config.sh

AGENT_LOG_PATH="${GEAFLOW_LOG_DIR}/agent.log"
AGENT_TMP_DIR=/tmp/agent

AGENT_MAIN_CLASS="com.antgroup.geaflow.dashboard.agent.runner.AgentWebRunner"

source /etc/profile
user=admin

function checkEngineJarDownloaded() {
    echo "Waiting for engine jars downloading ready..."
    cnt=0
    while [[ ! -f ${ENGINE_JAR_READY_FILE} ]]; do
      sleep 1
      cnt=`expr $cnt + 1`
      if [[ `expr $cnt % 30` == 0 ]]; then
        echo "Still waiting for engine jars ready... ${cnt}s"
      fi
    done
}

function startAgent() {
    export GEAFLOW_CLASSPATH=`buildEngineClassPath`
    AGENT_START_COMMAND="${JAVA_HOME}/bin/java -cp ${GEAFLOW_CLASSPATH} \
    -DGEAFLOW_AGENT_SERVER_PORT=${GEAFLOW_AGENT_SERVER_PORT} \
    -DGEAFLOW_DEPLOY_LOG_PATH=${DEPLOY_LOG_PATH} -DGEAFLOW_LOG_DIR=${GEAFLOW_LOG_DIR} \
    -DFLAME_GRAPH_PROFILER_PATH=${ASYNC_PROFILER_SHELL_PATH} \
    -DAGENT_TMP_DIR=${AGENT_TMP_DIR} \
    ${AGENT_MAIN_CLASS} >> ${AGENT_LOG_PATH} 2>&1"

    echo "Start agent with command: $AGENT_START_COMMAND"
    su $user -c "eval $AGENT_START_COMMAND"
}

createDirIfNeed $GEAFLOW_LOG_DIR
createDirIfNeed $AGENT_TMP_DIR

setPermission
checkEngineJarDownloaded
startAgent

