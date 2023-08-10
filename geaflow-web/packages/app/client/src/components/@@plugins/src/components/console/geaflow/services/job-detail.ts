import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

/**
 * 获取指定作业
 */
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
          id: "openpiece-geaflow.geaflow.services.job-detail.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list[0];
};
export const getApiTasks = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/logs`,
    {
      method: "get",
      credentials: "include",
      withCredentials: true,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
      credentials: "include",
      withCredentials: true,
      data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.SubmissionFailedResponsemessage",
          dm: "提交失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
      credentials: "include",
      withCredentials: true,
      data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.SaveFailedResponsemessage",
          dm: "保存失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
    credentials: "include",
    withCredentials: true,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.FailedToQueryTheCluster",
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
 * 提交作业
 */
export const getTaskIdStatus = async (taskId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/status`,
    {
      method: "get",
      credentials: "include",
      withCredentials: true,
      params: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
    credentials: "include",
    withCredentials: true,
    params: {
      resourceId,
      resourceType: "TASK",
      size: 10,
      sort: "id",
      order: "DESC",
      page: page,
    },
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/operations`,
    {
      method: "post",
      data: {
        action: "RESET",
      },
    }
  );

  if (!cleanDataResp?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.VerificationFailedResponsemessage",
          dm: "验证失败： {responseMessage}",
        },
        { responseMessage: cleanDataResp.message }
      )
    );
    return null;
  }
  return cleanDataResp;
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
      // data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.VerificationFailedResponsemessage",
          dm: "验证失败： {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
      // data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.VerificationFailedResponsemessage",
          dm: "验证失败： {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
      // data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.VerificationFailedResponsemessage",
          dm: "验证失败： {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response.data;
};
export const getContainer = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/heartbeat`,
    {
      method: "get",
      // data: params,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.VerificationFailedResponsemessage",
          dm: "验证失败： {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.FailedToObtainTheOffset",
          dm: "获取 Offset 列表失败： {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
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
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.job-detail.FailedToObtainHeartbeatData",
          dm: "获取heartbeat数据失败： {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response.data;
};
