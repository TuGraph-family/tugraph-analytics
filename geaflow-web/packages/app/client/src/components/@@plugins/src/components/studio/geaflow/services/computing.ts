import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";
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
    credentials: "include",
    withCredentials: true,
    data: params,
  });

  return response;
};

/**
 * 编辑图计算数据
 */
export const getJobsEditList = async (id: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${id}`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
  });

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
      credentials: "include",
      withCredentials: true,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.PublishingFailedResponsemessage",
          dm: "发布失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response;
};
export const deleteComputing = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${jobId}`, {
    method: "delete",
  });

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
export const getJobsTasks = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tasks`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    params: { jobId },
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};
export const getRemoteFiles = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    requestType: "form",
  });
  return response?.data?.list;
};
