import request from "./request";
import { HTTP_SERVICE_URL } from "../../constants";
import { message } from "antd";
import $i18n from "@/components/i18n";

export const getPlugins = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/plugins`, {
    method: "GET",
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

export const createPlugins = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/plugins`, {
    method: "post",
    requestType: "form",
    data: params,
  });
  return response;
};
export const getRemoteFiles = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    requestType: "form",
  });
  return response?.data?.list;
};

export const deletePlugin = async (pluginId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/plugins/${pluginId}`,
    {
      method: "DELETE",
    }
  );

  if (response.code !== "SUCCESS") {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.function-manage.FailedToDeleteResponsemessage",
          dm: "删除失败：{responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};
