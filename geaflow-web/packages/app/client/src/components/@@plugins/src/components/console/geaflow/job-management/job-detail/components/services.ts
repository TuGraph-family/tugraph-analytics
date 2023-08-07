import request from "../../../services/request";
import { HTTP_SERVICE_URL } from "../../../constants";
import { message } from "antd";

export const getMetriclist = async (taskId: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/tasks/${taskId}/metrics`,
    {
      method: "post",
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
    }
  );

  if (!response?.success) {
    message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response?.data?.list;
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
export const getConfigJob = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/config/job`, {
    method: "get",
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
  });

  if (!response?.success) {
    message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response?.data;
};
