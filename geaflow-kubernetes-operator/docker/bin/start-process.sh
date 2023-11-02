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

set -x

source /etc/profile

# prepare path
BASE_LOG_DIR=/tmp/logs
GEAFLOW_LOG_DIR=$BASE_LOG_DIR/geaflow
mkdir -p $BASE_LOG_DIR
mkdir -p $GEAFLOW_LOG_DIR
if [[ ! -L $GEAFLOW_HOME/logs ]]; then
  ln -s $BASE_LOG_DIR $GEAFLOW_HOME/logs
fi

function startGeaflowOperator() {
  K8S_SERVICE_ACCOUNT=${K8S_SERVICE_ACCOUNT:-"default"}
  PID_FILE=$GEAFLOW_HOME/geaflow.pid
  ps -p $(cat $PID_FILE 2>/dev/null) &> /dev/null && echo "geaflow-operator has been started" || {
    cd $GEAFLOW_HOME
    nohup $JAVA_HOME/bin/java -Xmx2048m -cp $GEAFLOW_HOME/boot/geaflow-kubernetes-operator-bootstrap.jar \
      org.springframework.boot.loader.JarLauncher --java_8_home=$JAVA_8_HOME \
      --is_local_mode=$IS_LOCAL_MODE --k8s_master_url=$K8S_MASTER_URL \
      --server.port=$HTTP_SERVER_PORT \
       > $GEAFLOW_LOG_DIR/stdout.log 2>$GEAFLOW_LOG_DIR/stderr.log &
    echo "start geaflow success"
    echo $! >$PID_FILE
  }
}

# start geaflow-operator
startGeaflowOperator || exit 1

# work dir
cd $GEAFLOW_HOME && echo "work directory: $(pwd)"

# docker entrypoint
tail -f /dev/null
