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

source /etc/profile

# prepare path
CONFIG_DIR="$GEAFLOW_HOME/config"
CONFIG_FILE="$CONFIG_DIR/application.properties"
BASE_LOG_DIR=/tmp/logs
GEAFLOW_LOG_DIR=$BASE_LOG_DIR/geaflow
GEAFLOW_TASK_LOG_DIR=$BASE_LOG_DIR/task
GEAFLOW_WEB_LOG_DIR=$BASE_LOG_DIR/geaflow-web
REDIS_LOG_DIR=$BASE_LOG_DIR/redis
INFLUXDB_LOG_DIR=$BASE_LOG_DIR/influxdb
mkdir -p $BASE_LOG_DIR
mkdir -p $GEAFLOW_LOG_DIR
mkdir -p $GEAFLOW_TASK_LOG_DIR
mkdir -p $GEAFLOW_WEB_LOG_DIR
mkdir -p $REDIS_LOG_DIR
mkdir -p $INFLUXDB_LOG_DIR
if [[ ! -L $GEAFLOW_HOME/logs ]]; then
  ln -s $BASE_LOG_DIR $GEAFLOW_HOME/logs
fi

# rewrite config
params=(
  "geaflow.deploy.mode"
  "geaflow.host"
  "geaflow.web.port"
  "geaflow.gateway.port"
  "geaflow.web.url"
  "geaflow.web.gateway.url"
  "geaflow.gateway.url"
  "spring.datasource.driver-class-name"
  "spring.datasource.url"
  "spring.datasource.username"
  "spring.datasource.password"
)
for param in ${params[*]}; do
  value=`env | grep "${param}="`
  if [ ${value} ]; then
      echo "rewrite property by docker env param: ${value}"
      sed -i "s/${param}=.*/$(echo $value | sed 's/\([\/\&\^]\)/\\\1/g')/g" $CONFIG_FILE
  fi
done

# parse deploy config
while read line; do
  if [[ "$line" =~ (.*)=(.*) ]]; then
    KEY=${BASH_REMATCH[1]}
    VALUE=${BASH_REMATCH[2]}
    if [ "$KEY" == "geaflow.deploy.mode" ]; then
      DEPLOY_MODE=$VALUE
    elif [ "$KEY" == "geaflow.host" ]; then
      GEAFLOW_HOST=$VALUE
    elif [ "$KEY" == "geaflow.web.port" ]; then
      GEAFLOW_WEB_PORT=$VALUE
    elif [ "$KEY" == "geaflow.gateway.port" ]; then
      GEAFLOW_GATEWAY_PORT=$VALUE
    elif [ "$KEY" == "geaflow.web.url" ]; then
      GEAFLOW_WEB_URL=$VALUE
    elif [ "$KEY" == "geaflow.web.gateway.url" ]; then
      GEAFLOW_WEB_GATEWAY_URL=$VALUE
    fi
  fi
done < $CONFIG_FILE

if [ "$GEAFLOW_WEB_URL" == "" ]; then
  GEAFLOW_WEB_URL="http://${GEAFLOW_HOST}:${GEAFLOW_WEB_PORT}"
else
  GEAFLOW_WEB_URL=$(echo ${GEAFLOW_WEB_URL} | \
    sed "s#\${geaflow.host}#$GEAFLOW_HOST#g" | \
    sed "s#\${geaflow.web.port}#$GEAFLOW_WEB_PORT#g")
fi
echo -e "geaflow.web.url = ${GEAFLOW_WEB_URL}"

if [ "$GEAFLOW_WEB_GATEWAY_URL" == "" ]; then
  GEAFLOW_WEB_GATEWAY_URL="http://${GEAFLOW_HOST}:${GEAFLOW_GATEWAY_PORT}"
else
  GEAFLOW_WEB_GATEWAY_URL=$(echo ${GEAFLOW_WEB_GATEWAY_URL} | \
    sed "s#\${geaflow.host}#$GEAFLOW_HOST#g" | \
    sed "s#\${geaflow.gateway.port}#$GEAFLOW_GATEWAY_PORT#g")
fi
echo -e "geaflow.web.gateway.url = ${GEAFLOW_WEB_GATEWAY_URL}"

function startMysql() {
    bash $GEAFLOW_HOME/bin/start-mysql.sh
}

function startRedis() {
  /usr/bin/redis-cli ping &> /dev/null && echo "redis has been started" || {
    echo 'starting redis...'
    nohup /usr/bin/redis-server --protected-mode no >> $REDIS_LOG_DIR/stdout.log \
      2>> $REDIS_LOG_DIR/stderr.log &
  }
}

function startInfluxdb() {
  /usr/local/bin/influx ping &> /dev/null && echo "influxdb has been started" || {
    echo 'starting influxdb...'
    nohup /usr/bin/influxd >> $INFLUXDB_LOG_DIR/stdout.log \
      2>> $INFLUXDB_LOG_DIR/stderr.log &
  }
}

function startGeaflowWeb() {
  if [[ "$(ps aux | grep 'yarn start' | grep -v 'grep' | wc -l)" = "0" ]]; then
    cd $GEAFLOW_WEB_HOME/

    # config web port
    sed -i "s/APP_PORT=.*/APP_PORT=${GEAFLOW_WEB_PORT}/g" .env

    # config web gateway url
    SED_GEAFLOW_WEB_GATEWAY_URL=$(echo $GEAFLOW_WEB_GATEWAY_URL | sed 's/\//\\\//g')
    SED_ACTION=$(echo "s/window.GEAFLOW_HTTP_SERVICE_URL.*;/window.GEAFLOW_HTTP_SERVICE_URL = '$SED_GEAFLOW_WEB_GATEWAY_URL';/g")
    sed -i "$SED_ACTION" packages/app/client/dist/index.html

    # start web
    yarn start > $GEAFLOW_WEB_LOG_DIR/stdout.log 2>$GEAFLOW_WEB_LOG_DIR/stderr.log &
    echo "start geaflow-web success"
  else
    echo "geaflow-web has been started"
  fi
}

function startGeaflowConsole() {
  PID_FILE=$GEAFLOW_HOME/geaflow.pid
  ps -p $(cat $PID_FILE 2>/dev/null) &> /dev/null && echo "geaflow-console has been started" || {
    cd $GEAFLOW_HOME
    nohup java -Xmx4096m -cp $GEAFLOW_HOME/boot/geaflow-console-bootstrap.jar:$CONFIG_DIR \
      org.springframework.boot.loader.JarLauncher > $GEAFLOW_LOG_DIR/stdout.log \
      2>$GEAFLOW_LOG_DIR/stderr.log &
    echo "start geaflow success"
    echo $! >$PID_FILE
  }
}

# start mysql, redis, influxdb
if [ "$DEPLOY_MODE" == "local" ]; then
  startMysql || exit 1
  startRedis || exit 1
  startInfluxdb || exit 1
fi

# start geaflow-web
startGeaflowWeb || exit 1

# start geaflow-console
startGeaflowConsole || exit 1

# work dir
cd $GEAFLOW_HOME && echo "work directory: $(pwd)"

# docker entrypoint
tail -f /dev/null
