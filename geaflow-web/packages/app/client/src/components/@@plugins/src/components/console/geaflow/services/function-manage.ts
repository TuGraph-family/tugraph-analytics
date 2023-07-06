import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

export const getFunctions = async (
  instanceName: string,
  functionName?: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/functions/`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      params: { functionName },
    }
  );

  if (!response?.success) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response?.data.list;
};

export const createFunction = async (instanceName: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/functions`,
    {
      method: "post",
      headers: {
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      requestType: "form",
      data: params,
    }
  );
  return response;
};
export const getRemoteFiles = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    headers: {
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    requestType: "form",
  });
  return response?.data?.list;
};

export const deleteFunction = async (
  instanceName: string,
  functionName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/functions/${functionName}`,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (response.code !== "SUCCESS") {
    message.error(`删除失败：${response.message}`);
    return null;
  }
  return response;
};
