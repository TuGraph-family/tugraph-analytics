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


export GEAFLOW_HOME=${GEAFLOW_HOME:-"/opt/geaflow/"}
export GEAFLOW_LOG_DIR=${GEAFLOW_LOG_DIR:-"/home/admin/logs/geaflow"}
export ENGINE_JAR_READY_FILE="/home/admin/logs/engine_jar_ready_flag"
export DEPLOY_LOG_PATH="/home/admin/logs/geaflow/deploy.log"

function createDirIfNeed() {
  if [ ! -d $1 ]; then
    mkdir -p -m 755 $1
  fi
  chown -R $user:$user $1 >/dev/null 2>&1
}

function buildEngineClassPath() {
    local GEAFLOW_CLASSPATH

    while read -d '' -r jarfile ; do
         GEAFLOW_CLASSPATH="$GEAFLOW_CLASSPATH":"$jarfile"
    done < <(find "$GEAFLOW_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ "$GEAFLOW_CLASSPATH" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] geaflow engine jar not found in $GEAFLOW_LIB_DIR.")

        # exit function with empty classpath to force process failure
        sleep 1
        exit 1
    fi
    GEAFLOW_CLASSPATH="${GEAFLOW_CONF_DIR}:${GEAFLOW_CLASSPATH}"
    echo "$GEAFLOW_CLASSPATH""$GEAFLOW_FAT_JAR"
}

function setPermission() {
    if [ $GEAFLOW_PERSISTENT_ROOT ]; then
      createDirIfNeed $GEAFLOW_PERSISTENT_ROOT
    fi
    chown -R $user:$user /home/$user/ >/dev/null 2>&1
}