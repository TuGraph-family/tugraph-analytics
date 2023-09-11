import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

interface GraphDefinitionParams {
  instanceName: string;
  page?: number;
  name?: string;
}

/**
 * GraphView
 */
export const getGraphDefinitionList = async (params: GraphDefinitionParams) => {
  const { instanceName, ...others } = params;
  console.log(others);
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/graphs`,
    {
      method: "get",
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
 * 图删除
 */
export const deleteGraphDefinition = async (
  instanceName: string,
  graphName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/graphs/${graphName}`,
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
 * 获取插件类型
 * @param type 类型名称
 * @returns
 */
export const getPluginCategoriesByType = (type: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types`,
    {
      method: "GET",
    }
  );
};

export const getPluginCategoriesConfig = (type: string, value: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types/${value}`,
    {
      method: "GET",
    }
  );
};

interface CreateGraphField {
  fields: {
    category: string;
    name: string;
    type: string;
    comment: string;
  }[];
  name: string;
  type: string;
}

interface CrateGraphParams {
  name: string;
  edges: CreateGraphField[];
  vertices: CreateGraphField[];
  pluginConfig: {
    type: string;
    config: any;
  };
}

/**
 * 创建图定义
 * @param instanceName 实例名称
 * @param params 创建图的参数
 * @returns
 */
export const createGraphDefinition = (instanceName: string, params: any) => {
  return request(`${HTTP_SERVICE_URL}/api/instances/${instanceName}/graphs`, {
    method: "POST",
    data: params,
  });
};

/**
 * 创建图定义
 * @param instanceName 实例名称
 * @param params 创建图的参数
 * @returns
 */
export const updateGraphDefinition = (
  instanceName: string,
  graphName: string,
  params: any
) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/graphs/${graphName}`,
    {
      method: "PUT",
      data: params,
    }
  );
};

export const graphDetail = (instanceName: string, graphName: string) => {
  return request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/graphs/${graphName}`,
    {
      method: "get",
    }
  );
};
