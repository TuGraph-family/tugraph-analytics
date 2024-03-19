import request from "./request";
import { HTTP_SERVICE_URL } from "../../constants";
import { message } from "antd";
import $i18n from "@/components/i18n";

export const getLanguageList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/llms`, {
    method: "get",
    params: params,
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.file-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response?.message }
      )
    );
    return [];
  }
  return response?.data.list;
};

export const deleteLanguage = async (name: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/llms/${name}`, {
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

export const createLanguage = async (params) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/llms`, {
    method: "post",
    data: params,
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.use-manage.FailedToAddResponsemessage",
          dm: "新增失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response?.data;
};

export const UpdateLanguage = async (params, name: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/llms/${name}`, {
    method: "put",
    data: params,
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.use-manage.FailedToAddResponsemessage",
          dm: "编辑失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};

export const llmsCall = async (params) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/chats/callSync`, {
    method: "post",
    params: params,
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.use-manage.FailedToAddResponsemessage",
          dm: "测试失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};

export const getTypes = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/llms/types`, {
    method: "get",
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.file-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response?.message }
      )
    );
    return [];
  }
  return response?.data;
};
