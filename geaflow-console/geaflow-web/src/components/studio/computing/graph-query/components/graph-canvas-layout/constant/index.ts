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

import type { Layout } from "@antv/graphin";

export const CANVAS_LAYOUT: {
  layout: Layout;
  title: string;
  icon: string;
}[] = [
  {
    layout: {
      type: "graphin-force",
      animation: false,
    },
    title: "力导布局",
    icon: "icon-yuanxingbuju",
  },
  {
    layout: {
      type: "concentric",
      preventOverlap: true,
      nodeSize: 150,
    },
    title: "同心圆布局",
    icon: "icon-tongxinyuanbuju",
  },
  {
    layout: {
      type: "grid",
    },
    title: "栅格布局",
    icon: "icon-jingdianlidaoxiangbuju",
  },
];
