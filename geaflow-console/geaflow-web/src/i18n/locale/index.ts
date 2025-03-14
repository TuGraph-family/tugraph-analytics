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

import antdEnUS from "antd/lib/locale/en_US";
import antdZhCN from "antd/lib/locale/zh_CN";
import enUS from "./en_US";
import zhCN from "./zh_CN";

export type LocaleOptions = {
  label: string;
  moment: string;
  antd: any;
  resources?: any;
};

export default {
  "en-US": {
    label: "English",
    // https://github.com/moment/moment/blob/develop/locale/en.js
    moment: "en",
    // https://github.com/ant-design/ant-design/tree/master/components/locale/en_US
    antd: antdEnUS,
    resources: {
      client: {
        ...enUS,
      },
    },
  },
  "zh-CN": {
    label: "简体中文",
    // https://github.com/moment/moment/blob/develop/locale/zh-cn.js
    moment: "zh-cn",
    // https://github.com/ant-design/ant-design/tree/master/components/locale/zh_CN
    antd: antdZhCN,
    // i18next
    resources: {
      client: {
        ...zhCN,
      },
    },
  },
} as Record<string, LocaleOptions>;
