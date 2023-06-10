import request from 'umi-request';
import { HTTP_SERVICE_URL } from '../constants';
import { message } from 'antd';

/**
 * 获取所有作业的列表
 */
export const getApiTasks = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tasks`, {
    method: 'get',
    params: params,
    headers: { 'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN') },
    credentials: 'include',
    withCredentials: true,
  });

  if (!response.success) {
    message.error(`查询作业列表失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};

/**
 * 获取集群
 */
export const getApiClusters = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/clusters`, {
    method: 'get',
    headers: { 'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN') },
    credentials: 'include',
    withCredentials: true,
  });

  if (!response.success) {
    message.error(`查询集群失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};

/**
 * 获取版本
 */
export const getApiVersions = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/versions`, {
    method: 'get',
    headers: { 'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN') },
    credentials: 'include',
    withCredentials: true,
  });

  if (!response.success) {
    message.error(`查询集群失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};

/**
 * 获取实例
 */
export const getApiInstances = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: 'get',
    headers: { 'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN') },
    credentials: 'include',
    withCredentials: true,
  });

  if (!response.success) {
    message.error(`查询集群失败: ${response.message}`);
    return [];
  }
  return response?.data?.list;
};
