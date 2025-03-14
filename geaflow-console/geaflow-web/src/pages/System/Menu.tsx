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
    key: "/system",
    label: $i18n.get({
      id: "openpiece-geaflow.SYSTEM",
      dm: "系统管理",
    }),
    icon: <ControlOutlined />,
    children: [
      {
        key: "/system/ColClusterManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Clusters",
          dm: "集群管理",
        }),
      },
      {
        key: "/system/ColGeaflowPlugManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Plugins",
          dm: "插件管理",
        }),
      },
      {
        key: "/system/ColLanguageManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Languages",
          dm: "模型管理",
        }),
      },
      {
        key: "/system/ColGeaflowJarfileManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Files",
          dm: "文件管理",
        }),
      },
      {
        key: "/system/ColVersionManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Versions",
          dm: "版本管理",
        }),
      },
      {
        key: "/system/ColUserManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Users",
          dm: "用户管理",
        }),
      },
      {
        key: "/system/ColTenantManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Tenants",
          dm: "租户管理",
        }),
      },
    ],
  },
];
