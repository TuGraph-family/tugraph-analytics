import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

interface GraphDefinitionParams {
  instanceName: string;
  page?: number;
  name?: string;
}

/**
 * GraphView
 */
export const getTableDefinitionList = async (params: GraphDefinitionParams) => {
  const { instanceName, ...others } = params;
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/tables`,
    {
      method: "get",
      headers: {
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      params: others,
    }
  );

  if (!response?.success || !response?.data) {
    // message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response.data?.list || [response.data];
};

/**
 * 表删除
 */
export const deleteTableDefinition = async (
  instanceName: string,
  tableName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/tables/${tableName}`,
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

/**
 * 创建图定义
 * @param instanceName 实例名称
 * @param params 创建图的参数
 * @returns
 */
export const createTableDefinition = (instanceName: string, params: any) => {
  return request(`${HTTP_SERVICE_URL}/api/instances/${instanceName}/tables`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    data: params,
  });
};

/**
 * 创建图定义
 * @param instanceName 实例名称
 * @param params 创建图的参数
 * @returns
 */
export const updateTableDefinition = (
  instanceName: string,
  tableName: string,
  params: any
) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/tables/${tableName}`,
    {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      data: params,
    }
  );
};
