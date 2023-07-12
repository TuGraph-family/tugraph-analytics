import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

export const getVersionList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: "GET",
    params: params,
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.version.QueryFailedResponsemessage",
          dm: "查询失败：{responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data.list;
};

export const createVersion = async (params) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: "POST",
    requestType: "form",
    data: params,
  });
  return response;
};

export const deleteVersion = async (versionName: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/versions/${versionName}`,
    {
      method: "DELETE",
    }
  );

  if (response.code !== "SUCCESS") {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.version.QueryFailedResponsemessage",
          dm: "查询失败：{responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return null;
  }
  return response;
};
