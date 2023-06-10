import request from 'umi-request';
import { HTTP_SERVICE_URL } from '../constants';
import { IDefaultValues } from '../quickInstall';

interface QuickInstallParams {
  dataConfig: IDefaultValues; 
  runtimeClusterConfig: IDefaultValues; 
  runtimeMetaConfig: IDefaultValues; 
  remoteFileConfig: IDefaultValues; 
  metricConfig: IDefaultValues; 
  haMetaConfig: IDefaultValues; 
}


/* default install params */
export const getQuickInstallParams = async () => {
  return request(`${HTTP_SERVICE_URL}/api/install`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
  });
}

/**
 * 一键安装
 * @param params 
 * @returns 
 */
export const quickInstallInstance = async (params: QuickInstallParams) => {
  return request(`${HTTP_SERVICE_URL}/api/install`, {
    method: 'POST',
    data: params,
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
  });
}

export const getPluginCategories = () => {
  return request(`${HTTP_SERVICE_URL}/api/config/plugin/categories`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
  });
}

/**
 * 获取插件类型
 * @param type 类型名称
 * @returns 
 */
export const getPluginCategoriesByType = (type: string) => {
  return request(`${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
  });
}

export const getPluginCategoriesConfig = (type: string, value: string) => {
  return request(`${HTTP_SERVICE_URL}/api/config/plugin/categories/${type}/types/${value}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
  });
}

/**
 * 判断是否已经执行过一键安装操作
 * @returns 
 */
export const hasQuickInstall = () => {
  return request(`${HTTP_SERVICE_URL}/api/configs/geaflow.initialized/value`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
  });
}

/**
 * 一键安装完成后切换用户角色
 * @returns 
 */
export const switchUserRole = async () => {
  return request(`${HTTP_SERVICE_URL}/api/session/switch`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
    credentials: 'include',
    withCredentials: true,
  });
}