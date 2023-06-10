import { extend } from "umi-request";
import { HTTP_SERVICE_URL } from "../constants";
import { message, notification } from "antd";

const codeMessage: Record<number, string> = {
  200: '服务器成功返回请求的数据。',
  201: '新建或修改数据成功。',
  202: '一个请求已经进入后台排队（异步任务）。',
  204: '删除数据成功。',
  400: '发出的请求有错误，服务器没有进行新建或修改数据的操作。',
  401: '用户没有权限（令牌、用户名、密码错误）。',
  403: '用户得到授权，但是访问是被禁止的。',
  404: '发出的请求针对的是不存在的记录，服务器没有进行操作。',
  406: '请求的格式不可得。',
  410: '请求的资源被永久删除，且不会再得到的。',
  422: '当创建一个对象时，发生一个验证错误。',
  500: '服务器发生错误，请检查服务器。',
  502: '网关错误。',
  503: '服务不可用，服务器暂时过载或维护。',
  504: '网关超时。',
};

/** 异常处理程序 */
const errorHandler = (error: { response: Response }): Response => {
  const { response } = error;
  if (response && response.status) {
    const errorText = codeMessage[response.status] || response.statusText;
    const { status, url } = response;

    notification.error({
      message: `请求错误 ${status}: ${url}`,
      description: errorText,
    });
  } else if (!response) {
    notification.error({
      description: '您的服务发生异常，无法连接服务器',
      message: '服务异常',
      key: 'serverError',
      duration: null
    });
  }
  return response;
};

/** 配置request请求时的默认参数 */
const request = extend({
  errorHandler, // 默认错误处理
  credentials: 'include', // 默认请求是否带上cookie
});


/**
 * 根据实例名称删除实例
 * @param instanceName 实例名称
 */
export const deleteInstance = async (instanceName: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}`,
    {
      method: "delete",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
    }
  );

  if (!response.success) {
    message.error(`删除实例失败: ${response.message}`);
    return null;
  }
  return response;
};

interface CreateInstanceProps {
  name: string;
  comment?: string;
  tenantName?: string;
  tenantId?: string;
  creatorName?: string;
  creatorId?: string;
}

interface UpdateInstanceProps {
  name: string;
  comment?: string;
}

/**
 * 新增实例
 * @param params 创建实例的参数
 */
export const createInstance = async (params: CreateInstanceProps) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: "post",
    headers: {
      "Content-Type": "application/json",
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    data: params,
  });

  if (!response.success) {
    message.error(`新增实例失败: ${response.message}`);
    return null;
  }
  return response;
};

/**
 * 更新实例
 * @param params 创建实例的参数
 */
export const updateInstance = async (
  params: UpdateInstanceProps,
  instanceName: string
) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}`,
    {
      method: "put",
      headers: {
        "Content-Type": "application/json",
        "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
      },
      data: params,
    }
  );

  if (!response.success) {
    message.error(`更新实例失败: ${response.message}`);
    return null;
  }
  return response;
};

/**
 * 获取实例
 */
export const queryInstanceList = async (params?: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/instances`, {
    method: "get",
    headers: {
      "Content-Type": "application/json",
      "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
    },
    params: params,
  });

  return response;
};
