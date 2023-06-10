import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

export const getVersionList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    params: params,
  });

  if (!response?.success) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response?.data.list;
};

export const createVersion = async (params) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: "POST",
    headers: {
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    requestType: "form",
    data: params,
  });
  return response;
};

export const deleteVersion = async (versionName: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/versions/${versionName}`,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (response.code !== "SUCCESS") {
    message.error(`查询失败：${response.message}`);
    return null;
  }
  return response;
};
