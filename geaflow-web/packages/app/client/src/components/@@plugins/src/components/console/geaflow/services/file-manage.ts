import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

/**
 * 获取文件
 */
export const getRemoteFiles = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response.success) {
    message.error(`查询失败: ${response.message}`);
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
      headers: {
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (!response?.success) {
    message.error(`删除失败: ${response?.message}`);
    return [];
  }
  return response;
};

export const createUpload = async (remoteFileId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/remote-files/${remoteFileId}`,
    {
      method: "put",
      headers: {
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      requestType: "form",
      data: params,
    }
  );
  return response;
};
