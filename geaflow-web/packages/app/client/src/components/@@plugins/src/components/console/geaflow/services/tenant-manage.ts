import request from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";

/**
 * 获取文件
 */
export const getTenantList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tenants`, {
    method: "get",
    headers: { "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN") },
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response.success) {
    message.error(`查询失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};
