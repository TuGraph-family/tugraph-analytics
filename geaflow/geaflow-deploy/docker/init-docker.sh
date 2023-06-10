#!/bin/sh
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

# init environment for contain.

GEAFLOW_LOG_DIR=/home/admin/logs/geaflow
GEAFLOW_HOME=/opt/geaflow/

echo "admin   -    core  unlimited" >>/etc/security/limits.d/90-users.conf
echo "admin   -    nofile  655360" >>/etc/security/limits.d/90-users.conf
echo "admin   -    nproc  165535" >>/etc/security/limits.d/90-users.conf

function setPermission() {
  user=admin
  groupadd $user
  useradd $user -g $user
  chmod 755 /home/$user/
  chmod 755 /home/admin/logs
  chown -R $user:$user $GEAFLOW_HOME >/dev/null 2>&1
}

setPermission
