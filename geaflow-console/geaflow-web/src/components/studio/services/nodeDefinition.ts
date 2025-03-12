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

interface GraphDefinitionParams {
  page?: number;
  name?: string;
}

/**
 * 点表格
 */
export const getNodeDefinitionList = async (
  instanceName: string,
  params?: GraphDefinitionParams
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/vertices`,
    {
      method: "get",
      params,
    }
  );

  if (!response?.success) {
    return [];
  }
  return response.data?.list;
};

/**
 * 点删除
 */
export const deleteVerticeDefinition = async (
  instanceName: string,
  vertexName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/vertices/${vertexName}`,
    {
      method: "delete",
    }
  );

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.function-manage.FailedToDeleteResponsemessage",
          dm: "删除失败：{responseMessage}",
        },
        { responseMessage: response?.message }
      )
    );
    return [];
  }
  return response;
};

/**
 * 创建图定义
 * @param instanceName 实例名称
 * @param params 创建图的参数
 * @returns
 */
export const createVerticeDefinition = (instanceName: string, params: any) => {
  return request(`${HTTP_SERVICE_URL}/api/instances/${instanceName}/vertices`, {
    method: "POST",
    data: params,
  });
};

export const verticeDetail = (instanceName: string, vertexName: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/vertices/${vertexName}`,
    {
      method: "get",
    }
  );
};

export const UpdateVerticeDefinition = (
  instanceName: string,
  vertexName: string,
  params: any
) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/vertices/${vertexName}`,
    {
      method: "put",
      data: params,
    }
  );
};
