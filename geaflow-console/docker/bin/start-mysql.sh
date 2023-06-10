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


SQL_FILE="$GEAFLOW_HOME/data/geaflow.sql"

if [[ "$(grep 'log-error=/var/lib/mysql/mysqld.log' /etc/my.cnf | wc -l)" = "0" ]]; then
  echo "config mysqld.log to /etc/my.cnf"
  sed -i '/^socket=.*/a log-error=/var/lib/mysql/mysqld.log' /etc/my.cnf
fi

mysql -e 'select 1' &> /dev/null && echo "mysql has been started" || {
  echo 'starting mysql...'
  nohup mysqld --user=mysql &>> /var/lib/mysql/mysqld.log &

  # check mysql started
  for (( i = 0; i < 30; i++ )); do
    mysql -e 'select 1' &> /dev/null && break
    echo 'waiting mysql started ...'
    sleep 1
  done

  mysql -e 'select 1' &> /dev/null || { echo 'start mysql failed'; exit 1; }
}

echo 'starting source sql file'
mysql < $SQL_FILE || { echo 'source sql file failed'; exit 1; }
echo 'source sql file finish'
