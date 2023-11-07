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

function checkDocker() {
  which docker &> /dev/null || { echo -e "\033[31mdocker is not installed\033[0m"; return 1; }
  docker version &> /dev/null || { echo -e "\033[31mdocker is not running\033[0m"; return 1; }
}

function checkMinikube() {
  checkDocker || return 1
  which minikube &> /dev/null || { echo -e "\033[31mminikube is not installed\033[0m"; return 1; }
  minikube status &> /dev/null || { echo -e "\033[31mminikube is not running\033[0m"; return 1; }
}

function checkMaven() {
  which java &> /dev/null || { echo -e "\033[31mjava is not installed\033[0m"; return 1; }
  which mvn &> /dev/null || { echo -e "\033[31mmaven is not installed\033[0m"; return 1; }
}

GEAFLOW_OPERATOR_DIR=$(pwd)
GEAFLOW_OPERATOR_TARGET_DIR=$GEAFLOW_OPERATOR_DIR/target
GEAFLOW_OPERATOR_IMAGE_NAME=geaflow-kubernetes-operator

# parse args
for arg in $*
do
  if [[ "$arg" = "--skipPackage" ]]; then
    SKIP_PACKAGE="true"
  elif [[ "$arg" = "--help" ]]; then
    HELP="true"
  else
    ERROR="$arg"
  fi
done

# print help message
if [[ -n "$ERROR" ]]; then
  echo -e "\033[31millegal argument found: $ERROR\033[0m"
fi
if [[ -n "$HELP" || -n "$ERROR" ]]; then
  echo -n 'Usage: build-operator.sh [-options]
Options:
    --skipPackage               Skip Build package.
    --help                      Show this help message.
'
  exit 1
fi

# prepare config
if [[ -z "$SKIP_PACKAGE" ]]; then
  checkMaven || exit 1
fi
checkDocker || exit 1

function buildGeaflowParent() {
  echo -e "\033[32mbuilding geaflow parent...\033[0m"
  mvn clean install -N -f=../pom.xml || return 1
  echo -e "\033[32msuccess to build geaflow parent\033[0m"
}

function buildJarPackage() {
  echo -e "\033[32mrun maven build in $GEAFLOW_OPERATOR_DIR ...\033[0m"

  checkMaven || return 1

  cd $GEAFLOW_OPERATOR_DIR
  mvn clean install -DskipTests -Dcheckstyle.skip -T4 || return 1
}

function buildGeaflowOperatorImage() {
  echo -e "\033[32mbuild geaflow-kubernetes-operator image ...\033[0m"

  JAR=$(find $GEAFLOW_OPERATOR_TARGET_DIR -name 'geaflow-kubernetes-operator-bootstrap-*-executable.jar')
  if [[ ! -f $JAR ]]; then
    echo -e "\033[31mgeaflow-kubernetes-operator jar not found\033[0m"
    return 1
  fi

  cd $GEAFLOW_OPERATOR_DIR

  checkMinikube
  MINIKUBE_INSTALLED=$?

  if [[ $MINIKUBE_INSTALLED = "0" ]]; then
    echo "build geaflow kubernetes operator image in minikube env"
    eval $(minikube docker-env 2> /dev/null) &> /dev/null
    docker build --network=host -t $GEAFLOW_OPERATOR_IMAGE_NAME:0.1 .
    RETURN_CODE=$?
    eval $(minikube docker-env --unset 2> /dev/null) &> /dev/null
  else
    echo -e '\033[31mbuild geaflow kubernetes operator image in local env\033[0m'
    docker build --network=host -t $GEAFLOW_OPERATOR_IMAGE_NAME:0.1 .
    RETURN_CODE=$?
  fi

  return $RETURN_CODE
}
# build image
if [[ -z "$SKIP_PACKAGE" ]]; then
  buildGeaflowParent || exit $?
  buildJarPackage || exit $?
fi
buildGeaflowOperatorImage || exit $?
echo -e "\033[32mbuild success !\033[0m"
