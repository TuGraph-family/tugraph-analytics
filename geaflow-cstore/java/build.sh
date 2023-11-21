#!/bin/bash
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

set -e

cd ..
make build-release
rm -f java/src/main/resources/libgeaflow_cstore.*
mkdir -p java/src/main/resources
ARCH=$(uname -m)
echo "arch:" $ARCH
echo "os:" $OSTYPE

if [[ "$OSTYPE" == "linux"* || "$OSTYPE" == "Linux"*  ]]; then
  cp target/release/libgeaflow_cstore.so java/src/main/resources/libgeaflow_cstore-${ARCH}.so
elif [[ "$OSTYPE" == "darwin"* || "$OSTYPE" == "Darwin"* ]]; then
  cp target/release/libgeaflow_cstore.dylib java/src/main/resources/libgeaflow_cstore-${ARCH}.dylib
else
  echo "WARN: unknown OSTYPE:" $OSTYPE
fi

echo "mvn clean install"
cd -
mvn clean install
