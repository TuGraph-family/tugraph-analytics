import request from "./request";
import { HTTP_SERVICE_URL } from "../constants";
import { message } from "antd";
import $i18n from "../../../../../../i18n";

export const getClustersList = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/clusters`, {
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

export const updateClusters = (clusterName: string, params: any) => {
  return request(`${HTTP_SERVICE_URL}/api/clusters/${clusterName}`, {
    method: "PUT",
    data: params,
  });
};

export const deleteClusters = async (clusterName: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/clusters/${clusterName}`,
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
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response;
};

export const createCluster = async (params) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/clusters`, {
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
