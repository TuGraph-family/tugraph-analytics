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

# base image
FROM docker.io/library/centos:7@sha256:9d4bcbbb213dfd745b58be38b13b996ebb5ac315fe75711bd618426a630e0987

VOLUME /tmp

ENV GEAFLOW_INSTALL_PATH=/opt
ENV GEAFLOW_HOME=$GEAFLOW_INSTALL_PATH/geaflow

# base packages
RUN yum -y install wget ps vim curl

# jdk11
RUN yum -y install java-11-openjdk java-11-openjdk-devel \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk" >> /etc/profile \
    && echo "export JRE_HOME=\${JAVA_HOME}/jre" >> /etc/profile \
    && echo "export CLASSPATH=.:\${JAVA_HOME}/lib:\${JRE_HOME}/lib:\$CLASSPATH" >> /etc/profile \
    && echo "export JAVA_PATH=\${JAVA_HOME}/bin:\${JRE_HOME}/bin" >> /etc/profile \
    && echo "export PATH=\$PATH:\${JAVA_PATH}" >> /etc/profile \
    && source /etc/profile

# jdk8
RUN yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel \
    && echo "export JAVA_8_HOME=/usr/lib/jvm/java-1.8.0-openjdk" >> /etc/profile \
    && source /etc/profile

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV JAVA_8_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV USE_LOCAL_K8S=false
ENV K8S_MASTER_URL=http://127.0.0.1:8000/
ENV HTTP_SERVER_PORT=8080


# copy jar
WORKDIR $GEAFLOW_HOME/
COPY target/boot/geaflow-kubernetes-operator-bootstrap-*-executable.jar \
    $GEAFLOW_HOME/boot/geaflow-kubernetes-operator-bootstrap.jar
COPY docker $GEAFLOW_HOME/

EXPOSE $HTTP_SERVER_PORT

ENTRYPOINT ["bash", "/opt/geaflow/bin/start-process.sh"]
