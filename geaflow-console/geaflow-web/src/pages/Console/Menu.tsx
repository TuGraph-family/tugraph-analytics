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

import { ControlOutlined } from "@ant-design/icons";
import React from "react";
import $i18n from "@/components/i18n";

export const MENU_List_MAP = [
  {
    key: "/console",
    label: $i18n.get({
      id: "openpiece-geaflow.OPERATION",
      dm: "运维中心",
    }),
    icon: <ControlOutlined />,
    children: [
      {
        key: "/console/ColJobList",
        label: $i18n.get({
          id: "openpiece-geaflow.Tasks",
          dm: "作业管理",
        }),
      },
      {
        key: "/console/ColGeaflowPlugManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Plugins",
          dm: "插件管理",
        }),
      },
      {
        key: "/console/ColInstanceList",
        label: $i18n.get({
          id: "openpiece-geaflow.Instances",
          dm: "实例管理",
        }),
      },
      {
        key: "/console/ColGeaflowJarfileManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Files",
          dm: "文件管理",
        }),
      },
      {
        key: "/console/ColUserManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Users",
          dm: "用户管理",
        }),
      },
    ],
  },
];
