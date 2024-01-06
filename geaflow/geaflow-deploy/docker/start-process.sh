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

CLUSTER_FAULT_INJECTION_ENABLE=${GEAFLOW_CLUSTER_FAULT_INJECTION_ENABLE:-"false"}

GEAFLOW_UDF_LIST=${GEAFLOW_UDF_LIST:-""}
GEAFLOW_ENGINE_JAR=${GEAFLOW_ENGINE_JAR:-""}
GEAFLOW_ALWAYS_DOWNLOAD_ENGINE_JAR=${GEAFLOW_ALWAYS_DOWNLOAD_ENGINE_JAR:-"false"}

echo "Remote engine jar files: $GEAFLOW_ENGINE_JAR"
echo "Always using remote engine jar files: $GEAFLOW_ALWAYS_DOWNLOAD_ENGINE_JAR"
echo "Udf jar list: $GEAFLOW_UDF_LIST"
echo "Udf jar path: $GEAFLOW_JAR_DOWNLOAD_PATH"

USE_DNS_FILTER=${USE_DNS_FILTER:-"true"}

# This file indicates that the initialization is complete
INIT_FILE="/tmp/geaflow-inited"

source /etc/profile
user=admin

function enableDnsFilter() {
  if [ "$USE_DNS_FILTER" == "true" -a ! -f "$INIT_FILE" ]; then
      sed -ci '/127.0.0.1/d' /etc/resolv.conf
      sed -ci '1i\nameserver 127.0.0.1' /etc/resolv.conf
      echo "Finished enable dns filter"
  fi
}

# 1. Download the engine jar package
function downloadEngineJars() {
  JARS=$(find ${GEAFLOW_LIB_DIR} -type f -name "[!.]*.jar" 2>/dev/null)
  if [ "$GEAFLOW_ALWAYS_DOWNLOAD_ENGINE_JAR" == "false" -a -n "${JARS}" ]; then
    echo "Default engine jar ${JARS} already exists, skip downloading engine jar."
  else
    if [ ! -f "$INIT_FILE" -a -n "${JARS}" ]; then
      cleanupLibDir
    fi
    createDirIfNeed $GEAFLOW_LIB_DIR
    su root -c "eval python $GEAFLOW_HOME/bin/udf-downloader.py $DEPLOY_LOG_PATH $GEAFLOW_LIB_DIR 'GEAFLOW_ENGINE_JAR' >> $DEPLOY_LOG_PATH 2>&1"
  fi
  echo "true" >${ENGINE_JAR_READY_FILE}
}

# 2. Download the udf and extract the zip
function downloadUdfJars() {
  createDirIfNeed $GEAFLOW_JAR_DOWNLOAD_PATH
  su root -c "eval python $GEAFLOW_HOME/bin/udf-downloader.py $DEPLOY_LOG_PATH $GEAFLOW_JAR_DOWNLOAD_PATH 'GEAFLOW_UDF_LIST' >> $DEPLOY_LOG_PATH 2>&1"
}

# Cleanup exist engine jars
function cleanupLibDir() {
  createDirIfNeed $GEAFLOW_LIB_DIR
  rm -rf $GEAFLOW_LIB_DIR
}

function buildCompleteClassPath() {

    local GEAFLOW_CLASSPATH
    GEAFLOW_CLASSPATH=`buildEngineClassPath`
    if [[ -z ${GEAFLOW_CLASSPATH} ]]; then
      echo ""
      exit 1
    fi

    # add UDF jars to classpath
    while read -d '' -r jarfile ; do
        GEAFLOW_CLASSPATH="$GEAFLOW_CLASSPATH":"$jarfile"
    done < <(find "$GEAFLOW_JAR_DOWNLOAD_PATH" ! -type d -name '[!.]*.jar' -print0 | sort -z)

    # add dir of unzipped UDF resource files(like gql/conf) to classpath
    GEAFLOW_CLASSPATH="$GEAFLOW_CLASSPATH":"$GEAFLOW_JAR_DOWNLOAD_PATH"

    echo "$GEAFLOW_CLASSPATH""$GEAFLOW_FAT_JAR"
}

function startProcess() {
    export GEAFLOW_CLASSPATH=`buildCompleteClassPath`
    if [[ -z ${GEAFLOW_CLASSPATH} ]]; then
      echo ""
      exit 1
    fi
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

createDirIfNeed $GEAFLOW_LOG_DIR
createDirIfNeed $GEAFLOW_LIB_DIR
createDirIfNeed $GEAFLOW_JOB_WORK_PATH
enableDnsFilter

downloadEngineJars
downloadUdfJars

checkIsRecover
touchInitFile

setPermission
startProcess

