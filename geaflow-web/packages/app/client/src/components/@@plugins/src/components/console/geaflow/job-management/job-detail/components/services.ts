import request from "umi-request";
import { HTTP_SERVICE_URL } from "../../../constants";
import { message } from "antd";

/**
 * 已有模版列表
 */
export const getGroupList = async (userAccount: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/user/group/list`, {
    method: "get",
    params: {
      userAccount,
    },
  });
  if (!response?.success) {
    console.log(response, "response");
    message.error(`查询已有模版: ${response?.message}`);
    return [];
  }
  return response?.data;
};

/**
 *   workers列表
 */
export const getWorkersList = async (
  jobId: string,
  search: string,
  sort: string,
  order: string,
  offset: number,
  limit: number
) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/workers/values`, {
    method: "get",
    params: {
      jobId,
      search,
      sort,
      order,
      offset,
      limit,
    },
  });

  if (!response?.rows) {
    console.log(response, "response");
    message.error(`查询events列表失败: ${response?.message}`);
    return [];
  }
  return response?.rows;
};

/**
 * online列表
 */
export const getTemplatesOnline = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/job/query/my/online`, {
    method: "get",
  });

  if (!response?.success) {
    message.error(`查询online列表失败： ${response?.message}`);
    return [];
  }
  return response?.data;
};

/**
 *   诊断列表
 */
export const getOptimizationList = async (jobId: string, version: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/diagnose/optimization/table`,
    {
      method: "get",
      params: {
        jobId,
        version,
      },
    }
  );

  // if (!response?.rows) {
  //   console.log(response, "response");
  //   message.error(`查询诊断列表失败: ${response?.message}`);
  //   return [];
  // }
  // return response?.rows;
};

/**
 *   调优列表
 */
export const getDiagnoseList = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/diagnose/table`, {
    method: "get",
    params: {
      jobId,
    },
  });

  // if (!response?.rows) {
  //   console.log(response, "response");
  //   message.error(`查询调优列表失败: ${response?.message}`);
  //   return [];
  // }
  // return response?.rows;
};
/**
 *   Metric图
 */
export const getMetricdata = async (params: DownLoadParams) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/100009/metricdata`, {
    method: "post",
    data: params,
  });

  if (!response?.success) {
    message.error(`下载Package失败: ${response?.message}`);
    return null;
  }
  return response?.data[0];
};

/**
 *   Dag列表
 */
export const getMetricsValues = async (
  jobId: string,
  search: string,
  sort: string,
  order: string,
  offset: number,
  limit: number
) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/workers/values`, {
    method: "get",
    params: {
      jobId,
      search,
      sort,
      order,
      offset,
      limit,
    },
  });

  if (!response?.rows) {
    message.error(`查询列表失败: ${response?.message}`);
    return "";
  }
  return response?.rows;
};

export const getJobMetricsValues = async (
  jobId: string,
  jobName: string,
  search: string,
  sort: string,
  order: string,
  offset: number,
  limit: number
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/job/metrics/values/${jobId}/${jobName}`,
    {
      method: "get",
      params: {
        search,
        sort,
        order,
        offset,
        limit,
      },
    }
  );

  if (!response?.rows) {
    message.error(`查询列表失败: ${response?.message}`);
    return "";
  }
  return response?.rows;
};

/**
 *   监控列表
 */
export const getMonitorValues = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/monitor`, {
    method: "get",
    params: {
      jobId,
    },
  });

  if (!response?.data) {
    message.error(`查询列表失败: ${response?.message}`);
    return [];
  }
  return response?.data;
};

export const getContainers = async (
  jobId: string,
  search: string,
  sort: string,
  order: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/job/exceptions/containers`,
    {
      method: "get",
      params: {
        jobId,
        search,
        sort,
        order,
      },
    }
  );

  if (!response?.data) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response?.data;
};

export const getTemplatesJobs = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/monitor`, {
    method: "get",
    params: {
      jobId,
    },
  });

  if (!response?.data) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response?.data;
};

export const getConfigValues = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/config/values`, {
    method: "get",
    params: {
      jobId,
    },
  });

  if (!response?.rows) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response.rows;
};

/**
 * 我的模版
 */
export const getTemplatesMy = async () => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/monitor/templates/my`,
    {
      method: "get",
    }
  );

  if (!response.success) {
    message.error(`查询我的模版失败： ${response.message}`);
    return [];
  }
  return response.data;
};

/**
 * 系统模版
 */
export const getTemplatesSystem = async () => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/monitor/templates/system`,
    {
      method: "get",
    }
  );

  if (!response.success) {
    message.error(`查询系统模版失败: ${response.message}`);
    return [];
  }
  return response.data;
};

/**
 * 绑定用户组
 */
export const getRelationBatch = async (params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/monitor/templates/job/relation/batch`,
    {
      method: "post",
      data: params,
    }
  );

  if (!response.success) {
    message.error(`绑定失败: ${response.message}`);
    return [];
  }
  return response;
};

/**
 * 修改模版
 */
export const applayTemplete = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/monitor/templates`, {
    method: "post",
    data: params,
  });

  if (!response?.success) {
    message.error(`新增失败: ${response?.message}`);
    return [];
  }
  return response;
};

/**
 * 修改模版
 */
export const deleteRelation = async (id: string, ids: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/monitor/templates/${id}/relation`,
    {
      method: "delete",
      params: { "ids[]": ids },
    }
  );

  if (!response?.success) {
    message.error(`解绑失败: ${response?.message}`);
    return [];
  }
  return response;
};

/**
 * 删除udf
 */
export const deleteUdf = async (id: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/jar/delete`, {
    method: "post",
    params: { id },
  });

  if (!response?.success) {
    message.error(`删除失败: ${response?.message}`);
    return [];
  }
  return response;
};

export const getMetricdataV = async (id: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/job/${id}/metricdata/v2`,
    {
      method: "get",
      params: params,
    }
  );

  if (!response?.data) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response;
};

export const getTrendValues = async (id: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/job/offset/trend/values/v2`,
    {
      method: "get",
      params: params,
    }
  );

  if (!response?.data) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response;
};

export const getMetriclist = async (taskId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/metrics`,
    {
      method: "post",
      headers: {
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      data: params,
    }
  );

  if (!response?.success) {
    return [];
  }
  return response?.data?.list;
};

export const getMetricMeta = async (taskId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/metric-meta`,
    {
      method: "get",
      headers: {
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (!response?.success) {
    message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response?.data?.list;
};

export const getOffsetData = async (jobId: string, timeZone: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/job/offset/detail/values`,
    {
      method: "get",
      params: {
        jobId,
        timeZone,
      },
    }
  );

  if (!response?.rows) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response?.rows;
};

export const getOffsetDelete = async (id: string, params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/${id}/offset`, {
    method: "put",
    data: params,
  });

  if (!response?.success) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response;
};

export const getOffsetTrend = async (params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/job/offset/trend/values`,
    {
      method: "post",
      params: params,
    }
  );

  if (!response?.success) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response;
};

export const getJobStatus = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/status`, {
    method: "post",
    params: params,
  });

  if (!response?.success) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response.data;
};

export const getJobCompile = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/job/compile`, {
    method: "post",
    params: params,
  });

  if (!response?.success) {
    message.error(`查询失败: ${response?.message}`);
    return [];
  }
  return response.data;
};

/**
 * 查询域管理员列表
 * @param admins 域管理员名称列表
 */
export const getDomainAdminList = async (admins: string[]) => {
  const response = await request(`${HTTP_SERVICE_URL}/user/accounts`, {
    method: "post",
    data: admins,
  });

  if (!response.success) {
    message.error(`查询域管理员列表失败: ${response.message}`);
    return [];
  }
  return response.data;
};

/**
 * 获取域用户列表
 * @param domainCode 域代码
 */
export const getDomainUserList = async (domainCode: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/user/query`, {
    method: "get",
    params: {
      domainCode,
    },
  });

  if (!response.success) {
    message.error(`查询域用户列表失败: ${response.message}`);
    return [];
  }
  return response.data;
};

/**
 * 获取插件类型
 * @param type 类型名称
 * @returns
 */
export const getPluginCategoriesByType = (type: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );
};

export const getPluginCategoriesConfig = (type: string, value: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types/${value}`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );
};
export const getConfigJob = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/config/job`, {
    method: "get",
    headers: {
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
  });

  if (!response?.success) {
    message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response?.data;
};
export const getConfigCluster = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/config/cluster`, {
    method: "get",
    headers: {
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
  });

  if (!response?.success) {
    message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response?.data;
};
