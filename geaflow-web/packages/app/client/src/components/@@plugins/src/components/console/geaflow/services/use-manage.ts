import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

/**
 * 获取用户
 */
export const getUsers = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users`, {
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
 * 新增用户
 */
export const getCreateUsers = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users`, {
    method: "post",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response.success) {
    message.error(`新增失败: ${response.message}`);
    return [];
  }
  return response;
};

/**
 * 删除用户
 */
export const deleteUser = async (userId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users/${userId}`, {
    method: "delete",
    headers: {
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
  });

  if (!response?.success) {
    message.error(`删除失败: ${response?.message}`);
    return [];
  }
  return response;
};

/**
 * 更新用户
 */
export const updateUser = async (userId: string, params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users/${userId}`, {
    method: "put",
    headers: {
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    data: params,
  });

  if (!response?.success) {
    message.error(`更新失败: ${response?.message}`);
    return [];
  }
  return response;
};
