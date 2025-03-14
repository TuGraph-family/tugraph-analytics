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
 * 获取文件
 */
export const getRemoteFiles = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.file-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response?.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};

/**
 * 下载文件
 */
export const getRemoteFileId = (remoteFileId: string) => {
  window.open(
    `${HTTP_SERVICE_URL}/api/remote-files/${remoteFileId}?download=true&geaflow-token=${localStorage.getItem(
      "GEAFLOW_TOKEN"
    )}`
  );
};

/**
 * 删除文件
 */
export const deleteRemoteFileId = async (remoteFileId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/remote-files/${remoteFileId}`,
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

export const createUpload = async (remoteFileId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/remote-files/${remoteFileId}`,
    {
      method: "put",
      requestType: "form",
      data: params,
    }
  );
  return response;
};
