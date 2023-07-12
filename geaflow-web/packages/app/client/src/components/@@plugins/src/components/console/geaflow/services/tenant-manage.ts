import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

/**
 * 获取文件
 */ 
export const getTenantList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tenants`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.tenant-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};
