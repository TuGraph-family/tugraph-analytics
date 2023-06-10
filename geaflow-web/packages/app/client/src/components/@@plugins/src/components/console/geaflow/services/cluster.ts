import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

export const getClustersList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/clusters`, {
    method: "get",
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

export const updateClusters = (clusterName: string, params: any) => {
  return request(`${HTTP_SERVICE_URL}/api/clusters/${clusterName}`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    data: params,
  });
};

export const deleteClusters = async (clusterName: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/clusters/${clusterName}`,
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

export const createCluster = async (params) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/clusters`, {
    method: "post",
    headers: {
      "Content-Type": "application/json",
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    data: params,
  });

  if (!response?.success) {
    message.error(`创建集群失败: ${response?.message}`);
    return null;
  }
  return response?.data;
};
