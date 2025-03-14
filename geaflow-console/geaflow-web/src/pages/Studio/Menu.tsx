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
    key: "/studio",
    label: $i18n.get({
      id: "openpiece-geaflow.DEVELOPMENT",
      dm: "图研发",
    }),
    icon: <ControlOutlined />,
    children: [
      {
        key: "/studio/StudioTableDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Tables",
          dm: "表定义",
        }),
      },
      {
        key: "/studio/StudioNodeDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Vertices",
          dm: "点定义",
        }),
      },
      {
        key: "/studio/StudioEdgeDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Edges",
          dm: "边定义",
        }),
      },
      {
        key: "/studio/StudioGraphDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Graphs",
          dm: "图定义",
        }),
      },
      {
        key: "/studio/StudioGeaflowFunctionManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Functions",
          dm: "函数定义",
        }),
      },
      {
        key: "/studio/StudioComputing",
        label: $i18n.get({
          id: "openpiece-geaflow.Jobs",
          dm: "图任务",
        }),
      },
    ],
  },
];
