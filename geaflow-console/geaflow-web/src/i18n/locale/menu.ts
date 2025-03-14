/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export const menuLocalConfig = {
  DEVELOPMENT: "图研发",
  Graphs: "图定义",
  Tables: "表定义",
  Edges: "边定义",
  Vertices: "点定义",
  Functions: "函数定义",
  Jobs: "图任务",
  OPERATION: "运维中心",
  Tasks: "作业管理",
  Instances: "实例管理",
  Files: "文件管理",
  Users: "用户管理",
  INSTALLATION: "一键安装",
  SYSTEM: "系统管理",
  Clusters: "集群管理",
  Tenants: "租户管理",
  Versions: "版本管理",
  Plugins: '插件管理'
};

const convertDict = (dictionary: any) => {
  let result = {
    "en-US": {},
    "zh-CN": {},
  } as any;
  for (let key in dictionary) {
    let value = dictionary[key];
    result["en-US"][value] = key;
    result["zh-CN"][value] = value;
  }
  return result;
};

export const menuZhAndEnText = convertDict(menuLocalConfig);
