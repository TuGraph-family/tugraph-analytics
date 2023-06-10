import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

/**
 * 获取指定作业
 */
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
  return response?.data?.list[0];
};
export const getApiTasks = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/logs`,
    {
      method: "get",
      headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
      credentials: "include",
      withCredentials: true,
    }
  );

  if (!response.success) {
    message.error(`查询失败: ${response.message}`);
    return [];
  }
  return response?.data;
};

/**
 * 提交作业
 */
export const getOperations = async (taskId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/operations`,
    {
      method: "post",
      headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
      credentials: "include",
      withCredentials: true,
      data: params,
    }
  );

  if (!response.success) {
    message.error(`提交失败: ${response.message}`);
    return [];
  }
  return response;
};
/**
 * 保存作业
 */
export const getReleases = async (jobId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/jobs/${jobId}/releases `,
    {
      method: "put",
      headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
      credentials: "include",
      withCredentials: true,
      data: params,
    }
  );

  if (!response.success) {
    message.error(`保存失败: ${response.message}`);
    return [];
  }
  return response;
};
/**
 * 获取版本
 */
export const getApiVersions = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: "get",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(`查询集群失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};
/**
 * 提交作业
 */
export const getTaskIdStatus = async (taskId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/status`,
    {
      method: "get",
      headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
      credentials: "include",
      withCredentials: true,
      params: params,
    }
  );

  if (!response.success) {
    message.error(`查询失败: ${response.message}`);
    return [];
  }
  return response;
};

/**
 * 查看指定作业的操作记录
 * @param jobId 作业ID
 */
export const getRecordList = async (resourceId: string, page: number) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/audits`, {
    method: "get",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    params: {
      resourceId,
      resourceType: "TASK",
      size: 10,
      sort: "gmt_create",
      order: "DESC",
      page: page,
    },
  });

  if (!response?.success) {
    message.error(`获取操作记录失败: ${response?.message}`);
    return null;
  }
  return response?.data;
};

/**
 * 重置作业任务
 * @param jobId 作业ID
 */
export const resetJob = async (taskId: string) => {
  const cleanDataResp = await request(
    `${HTTP_SERVICE_URL}/tasks/${taskId}/operations`,
    {
      method: "post",
      data: {
        action: "CLEAN_DATA",
      },
    }
  );

  if (!cleanDataResp?.success) {
    message.error(`重置数据失败: ${cleanDataResp?.message}`);
    return null;
  }

  const cleanMetaResp = await request(
    `${HTTP_SERVICE_URL}/tasks/${taskId}/operations`,
    {
      method: "post",
      data: {
        action: "CLEAN_META",
      },
    }
  );

  if (!cleanMetaResp?.success) {
    message.error(`重置元数据: ${cleanMetaResp?.message}`);
    return null;
  }
  return cleanMetaResp;
};

/**
 * 获取运行详情
 * @param params
 * @returns
 */
export const getJobRuntimeList = async (params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${params.id}/pipelines`,
    {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      // data: params,
    }
  );

  if (!response.success) {
    message.error(`验证失败： ${response.message}`);
    return [];
  }
  return response.data;
};

// /api/tasks/{taskId}/pipelines/{pipelinName}/cycles
export const getPipleinesCyclesByName = async (params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${params.id}/pipelines/${params.name.replace(
      "#",
      "%23"
    )}/cycles`,
    {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      // data: params,
    }
  );

  if (!response.success) {
    message.error(`验证失败： ${response.message}`);
    return [];
  }
  return response.data;
};

/**
 * 通过任务 ID 获取 Job 异常信息
 * @param taskId 任务 ID
 * @returns
 */
export const getJobErrorMessage = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/errors`,
    {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      // data: params,
    }
  );

  if (!response.success) {
    message.error(`验证失败： ${response.message}`);
    return [];
  }
  return response.data;
};
export const getContainer = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/heartbeat`,
    {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      // data: params,
    }
  );

  if (!response.success) {
    message.error(`验证失败： ${response.message}`);
    return [];
  }
  return response;
};
/**
 * 通过任务 ID 获取 Offset 信息
 * @param taskId 任务 ID
 * @returns
 */
export const getJobOffsetList = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/offsets`,
    {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (!response.success) {
    message.error(`获取 Offset 列表失败： ${response.message}`);
    return [];
  }
  return response.data?.list;
};

export const getHeartbeatByTaskId = async (taskId: string) => {
  // api/tasks/{taskId}/heartbeat
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/heartbeat`,
    {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (!response.success) {
    message.error(`获取heartbeat数据失败： ${response.message}`);
    return null;
  }
  return response.data;
};
