import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

export const getFunctions = async (
  instanceName: string,
  functionName?: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/functions/`,
    {
      method: "GET",
      params: { functionName },
    }
  );

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.file-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data.list;
};

export const createFunction = async (instanceName: string, params: any) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/functions`,
    {
      method: "post",
      requestType: "form",
      data: params,
    }
  );
  return response;
};
export const getRemoteFiles = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    requestType: "form",
  });
  return response?.data?.list;
};

export const deleteFunction = async (
  instanceName: string,
  functionName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/functions/${functionName}`,
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
