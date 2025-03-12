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
import { message } from "antd";
import $i18n from "@/components/i18n";

/**
 * 根据实例名称删除实例
 * @param instanceName 实例名称
 */
export const deleteInstance = async (instanceName: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}`,
    {
      method: "delete",
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.instance.FailedToDeleteTheInstance",
          dm: "删除实例失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};

interface CreateInstanceProps {
  name: string;
  comment?: string;
  tenantName?: string;
  tenantId?: string;
  creatorName?: string;
  creatorId?: string;
}

interface UpdateInstanceProps {
  name: string;
  comment?: string;
}

/**
 * 新增实例
 * @param params 创建实例的参数
 */
export const createInstance = async (params: CreateInstanceProps) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: "post",
    data: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.instance.FailedToAddAnInstance",
          dm: "新增实例失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};

/**
 * 更新实例
 * @param params 创建实例的参数
 */
export const updateInstance = async (
  params: UpdateInstanceProps,
  instanceName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}`,
    {
      method: "put",
      data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.instance.FailedToUpdateTheInstance",
          dm: "更新实例失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};

/**
 * 获取实例
 */
export const queryInstanceList = async (params?: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: "get",
    params: params,
  });

  return response;
};

export const getQueryInstanceList = (token: any) => {
  return request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: "GET",
    headers: {
      "geaflow-token": token,
    },
  });
};
