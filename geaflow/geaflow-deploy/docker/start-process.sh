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

CLUSTER_FAULT_INJECTION_ENABLE=${GEAFLOW_CLUSTER_FAULT_INJECTION_ENABLE:-"false"}

GEAFLOW_HOME=${GEAFLOW_HOME:-"/opt/geaflow/"}
GEAFLOW_LOG_DIR=${GEAFLOW_LOG_DIR:-"/home/admin/logs/geaflow"}
GEAFLOW_UDF_LIST=${GEAFLOW_UDF_LIST:-""}
GEAFLOW_ENGINE_JAR=${GEAFLOW_ENGINE_JAR:-""}

echo "engine jar files: $GEAFLOW_ENGINE_JAR"
echo "udf jar list: $GEAFLOW_UDF_LIST"
echo "udf jar path: $GEAFLOW_JAR_DOWNLOAD_PATH"

USE_DNS_FILTER=${USE_DNS_FILTER:-"true"}

# This file indicates that the initialization is complete
INIT_FILE="/tmp/geaflow-inited"

# The log file for this script
BASE_LOG_PATH="/home/admin/logs/geaflow.log"

source /etc/profile
user=admin

function enableDnsFilter() {
  if [ "$USE_DNS_FILTER" == "true" -a ! -f "$INIT_FILE" ]; then
      sed -ci '/127.0.0.1/d' /etc/resolv.conf
      sed -ci '1i\nameserver 127.0.0.1' /etc/resolv.conf
      echo "Finished enable dns filter"
  fi
}

# 1. Create log directory
function createDirIfNeed() {
  if [ ! -d $1 ]; then
    mkdir -p -m 755 $1
  fi
  chown -R $user:$user $1 >/dev/null 2>&1
}

# 2. Download the engine jar package
function downloadEngineJars() {
  createDirIfNeed $GEAFLOW_LIB_DIR
  su root -c "eval python /tmp/udf-downloader.py $BASE_LOG_PATH $GEAFLOW_LIB_DIR 'GEAFLOW_ENGINE_JAR' >> $BASE_LOG_PATH 2>&1"
}

# 3. Download the udf and extract the zip
function downloadUdfJars() {
  createDirIfNeed $GEAFLOW_JAR_DOWNLOAD_PATH
  su root -c "eval python /tmp/udf-downloader.py $BASE_LOG_PATH $GEAFLOW_JAR_DOWNLOAD_PATH 'GEAFLOW_UDF_LIST' >> $BASE_LOG_PATH 2>&1"
}

function buildClassPath() {
    local GEAFLOW_CLASSPATH

    while read -d '' -r jarfile ; do
         GEAFLOW_CLASSPATH="$GEAFLOW_CLASSPATH":"$jarfile"
    done < <(find "$GEAFLOW_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ "$GEAFLOW_CLASSPATH" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] geaflow engine jar not found in $GEAFLOW_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    # add UDF jars to classpath
    while read -d '' -r jarfile ; do
        GEAFLOW_CLASSPATH="$GEAFLOW_CLASSPATH":"$jarfile"
    done < <(find "$GEAFLOW_JAR_DOWNLOAD_PATH" ! -type d -name '*.jar' -print0 | sort -z)

    # add dir of unzipped UDF resource files(like gql/conf) to classpath
    GEAFLOW_CLASSPATH="$GEAFLOW_CLASSPATH":"$GEAFLOW_JAR_DOWNLOAD_PATH"

    echo "$GEAFLOW_CLASSPATH""$GEAFLOW_FAT_JAR"
}

function startProcess() {
    export GEAFLOW_CLASSPATH=`buildClassPath`
    echo "CLASSPATH:"$GEAFLOW_CLASSPATH

    echo "Start with command: $GEAFLOW_START_COMMAND"
    # Run from root to admin without -, keeping the current shell environment variables
    su $user -c "eval $GEAFLOW_START_COMMAND"
}

function touchInitFile() {
  touch $INIT_FILE
}

function checkIsRecover() {
    # If INIT_FILE already exists, it is determined to be FO Recover
    if [ -f "$INIT_FILE" ]; then
      export GEAFLOW_IS_RECOVER="true"

      # fo sleep 1 second each time
      sleep 1
    fi
}

function setPermission() {
    if [ $GEAFLOW_PERSISTENT_ROOT ]; then
      createDirIfNeed $GEAFLOW_PERSISTENT_ROOT
    fi
    chown -R $user:$user /home/$user/ >/dev/null 2>&1
}

createDirIfNeed $GEAFLOW_LOG_DIR
createDirIfNeed $GEAFLOW_JOB_WORK_PATH
enableDnsFilter

downloadEngineJars
downloadUdfJars

checkIsRecover
touchInitFile

setPermission
startProcess

