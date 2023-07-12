import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

/**
 * 获取所有作业的列表
 */ 
export const getApiTasks = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tasks`, {
    method: "get",
    params: params,
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-list.FailedToQueryJobList",
          dm: "查询作业列表失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};

/**
 * 获取集群
 */
export const getApiClusters = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/clusters`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-list.FailedToQueryTheCluster",
          dm: "查询集群失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};

/**
 * 获取版本
 */
export const getApiVersions = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-list.FailedToQueryTheCluster",
          dm: "查询集群失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};

/**
 * 获取实例
 */
export const getApiInstances = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-list.FailedToQueryTheCluster",
          dm: "查询集群失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};
