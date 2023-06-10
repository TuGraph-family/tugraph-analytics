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

# Find java binary.
if [ -z "$JAVA_HOME" ] ; then
  JAVACMD=`which java`
else
  JAVACMD="$JAVA_HOME/bin/java"
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "The JAVA_HOME environment variable is not defined correctly" >&2
  exit 1
fi

# Find geaflow home.
if [ ! -z "${GEAFLOW_HOME}" ]; then
  exit 0
else
  GEAFLOW_HOME="$(cd "$(dirname "$0")"; pwd)"
  if [ -d "${GEAFLOW_HOME}/../lib" ]; then
    export GEAFLOW_HOME="$(cd "${GEAFLOW_HOME}/.."; pwd)"
  else
    export GEAFLOW_HOME="$(cd "$GEAFLOW_HOME/../../../../../.."; pwd)"
  fi
fi
echo "GEAFLOW_HOME: $GEAFLOW_HOME"

# Find geaflow jars.
if [ -d "${GEAFLOW_HOME}/lib" ]; then
  export GEAFLOW_JAR_DIR="${GEAFLOW_HOME}/lib"
else
  export GEAFLOW_JAR_DIR="${GEAFLOW_HOME}/geaflow-deploy/geaflow-assembly/target"
fi
echo "GEAFLOW_JAR_DIR: $GEAFLOW_JAR_DIR"

# Find geaflow conf.
if [ -d "${GEAFLOW_HOME}/conf" ]; then
  export GEAFLOW_CONF_DIR="${GEAFLOW_HOME}/conf"
else
  export GEAFLOW_CONF_DIR="$(cd "$(dirname "$0")/../conf"; pwd)"
fi
echo "GEAFLOW_CONF_DIR: $GEAFLOW_CONF_DIR"
