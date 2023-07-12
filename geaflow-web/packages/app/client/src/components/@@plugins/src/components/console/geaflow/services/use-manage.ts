import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

/**
 * 获取用户
 */ 
export const getUsers = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.use-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};

/**
 * 新增用户
 */
export const getCreateUsers = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users`, {
    method: "post",
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.use-manage.FailedToAddResponsemessage",
          dm: "新增失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response;
};

/**
 * 删除用户
 */
export const deleteUser = async (userId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users/${userId}`, {
    method: "delete",
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.function-manage.FailedToDeleteResponsemessage",
          dm: "删除失败：{responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response;
};

/**
 * 更新用户
 */
export const updateUser = async (userId: string, params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/users/${userId}`, {
    method: "put",
    data: params,
  });

  if (!response?.success) {
    message.error(response?.message);
    return [];
  }
  return response;
};
