import request from 'umi-request';
import { HTTP_SERVICE_URL } from '../constants';

/* Login */
export const login = async (params: { username: string; password: string, systemLogin: boolean }) => {
  const { username, password, systemLogin } = params
  return request(`${HTTP_SERVICE_URL}/auth/login`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    data: {
      loginName: username,
      password,
      systemLogin
    },
  });
}

interface RegisterParams {
  name: string;
  comment: string;
  password: string;
  phone?: string;
  email?: string;
}
/**
 * 注册
 * @param params 
 * @returns 
 */
export const register = async (params: RegisterParams) => {
  return request(`${HTTP_SERVICE_URL}/auth/register`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    data: {
      ...params,
    },
  });
}

/* Logout */
export const logout = async () => {
  return request(`${HTTP_SERVICE_URL}/api/session/logout`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'geaflow-token': localStorage.getItem('GEAFLOW_TOKEN')
    },
    credentials: 'include',
    withCredentials: true,
  });
}
