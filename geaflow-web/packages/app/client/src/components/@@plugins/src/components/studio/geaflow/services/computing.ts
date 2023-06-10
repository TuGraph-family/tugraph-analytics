import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
interface ComputingParams {
  instanceId: string;
  name?: string;
}

/**
 * 查询图计算列表
 */
export const getJobsList = async (params: ComputingParams) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    params: params,
  });

  return response;
};

/**
 * 新增图计算
 */
export const getJobsCreat = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs`, {
    method: "post",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    data: params,
  });

  return response;
};

/**
 * 编辑图计算
 */
export const getJobsEdit = async (params: any, id: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${id}`, {
    method: "put",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    data: params,
  });

  if (!response.success) {
    message.error(`编辑失败: ${response.message}`);
    return [];
  }
  return response;
};

/**
 * 编辑图计算数据
 */
export const getJobsEditList = async (id: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${id}`, {
    method: "get",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(`编辑失败: ${response.message}`);
    return [];
  }
  return response;
};

/**
 * 发布图计算
 */
export const getJobsReleases = async (jobId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/jobs/${jobId}/releases`,
    {
      method: "post",
      headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
      credentials: "include",
      withCredentials: true,
    }
  );

  if (!response.success) {
    message.error(`发布失败: ${response.message}`);
    return [];
  }
  return response;
};
export const deleteComputing = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${jobId}`, {
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
export const getJobsTasks = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tasks`, {
    method: "get",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    params: { jobId },
  });

  if (!response.success) {
    message.error(`查询失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};
