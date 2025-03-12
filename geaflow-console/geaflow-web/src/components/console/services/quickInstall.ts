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

import request from "./request";
import { HTTP_SERVICE_URL } from "../../constants";
import { IDefaultValues } from "../quickInstall";

interface QuickInstallParams {
  dataConfig: IDefaultValues;
  runtimeClusterConfig: IDefaultValues;
  runtimeMetaConfig: IDefaultValues;
  remoteFileConfig: IDefaultValues;
  metricConfig: IDefaultValues;
  haMetaConfig: IDefaultValues;
}

/* default install params */
export const getQuickInstallParams = async () => {
  return request(`${HTTP_SERVICE_URL}/api/install`, {
    method: "GET",
  });
};

/**
 * 一键安装
 * @param params
 * @returns
 */
export const quickInstallInstance = async (params: QuickInstallParams) => {
  return request(`${HTTP_SERVICE_URL}/api/install`, {
    method: "POST",
    data: params,
  });
};

export const getPluginCategories = () => {
  return request(`${HTTP_SERVICE_URL}/api/config/plugin/categories`, {
    method: "GET",
  });
};

/**
 * 获取插件类型
 * @param type 类型名称
 * @returns
 */
export const getPluginCategoriesByType = (type: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types`,
    {
      method: "GET",
    }
  );
};

export const getPluginCategoriesConfig = (type: string, value: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types/${value}`,
    {
      method: "GET",
    }
  );
};

/**
 * 判断是否已经执行过一键安装操作
 * @returns
 */
export const hasQuickInstall = (token: string) => {
  return request(`${HTTP_SERVICE_URL}/api/configs/geaflow.initialized/value`, {
    method: "GET",
    headers: {
      "geaflow-token": token,
    },
  });
};

/**
 * 一键安装完成后切换用户角色
 * @returns
 */
export const switchUserRole = async () => {
  return request(`${HTTP_SERVICE_URL}/api/session/switch`, {
    method: "POST",
    credentials: "include",
    withCredentials: true,
  });
};

export const getSwitchUserRole = (token: any) => {
  return request(`${HTTP_SERVICE_URL}/api/session/switch`, {
    method: "POST",
    credentials: "include",
    withCredentials: true,
    headers: {
      "geaflow-token": token,
    },
  });
};
