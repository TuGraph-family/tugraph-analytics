#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get config
. "$bin"/config.sh

# build classpath
FAT_JAR=`ls $GEAFLOW_JAR_DIR | grep "^geaflow-assembly-.*[^sources].jar"`
CLASSPATH="$GEAFLOW_JAR_DIR/$FAT_JAR"
echo "CLASSPATH:$CLASSPATH"

$JAVACMD -cp "$CLASSPATH" org.apache.geaflow.cluster.k8s.client.KubernetesJobSubmitter "$@"


