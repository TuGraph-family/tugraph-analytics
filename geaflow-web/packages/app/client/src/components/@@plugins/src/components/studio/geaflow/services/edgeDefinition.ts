import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

interface GraphDefinitionParams {
  page?: number;
  name?: string;
}

/**
 * 点表格
 */
export const getEdgeDefinitionList = async (
  instanceName: string,
  params?: GraphDefinitionParams
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/edges`,
    {
      method: "get",
      params,
    }
  );

  if (!response?.success) {
    return [];
  }
  return response.data?.list;
};

/**
 * 点删除
 */
export const deleteEdgeDefinition = async (
  instanceName: string,
  vertexName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/edges/${vertexName}`,
    {
      method: "delete",
    }
  );

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

/**
 * 创建图定义
 * @param instanceName 实例名称
 * @param params 创建图的参数
 * @returns
 */
export const createEdgwDefinition = (instanceName: string, params: any) => {
  return request(`${HTTP_SERVICE_URL}/api/instances/${instanceName}/edges`, {
    method: "POST",
    data: params,
  });
};

export const EdgeDetail = (instanceName: string, vertexName: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/edges/${vertexName}`,
    {
      method: "get",
    }
  );
};

export const UpdateEdgeDefinition = (
  instanceName: string,
  vertexName: string,
  params: any
) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/edges/${vertexName}`,
    {
      method: "put",
      data: params,
    }
  );
};
