import { login, logout, register } from '../services/auth';
import { useRequest } from 'ahooks';

export const useAuth = () => {
  const { runAsync: onLogin, loading: loginLoading, error: loginError } = useRequest(login, { manual: true });
  const {
    runAsync: onRegister,
    loading: registerLoading,
    error: registerError,
  } = useRequest(register, {
    manual: true,
  });
  const { runAsync: onLogout, loading: logoutLoading, error: logoutError } = useRequest(logout, { manual: true });
  return {
    onLogin,
    loginLoading,
    loginError,
    onRegister,
    registerLoading,
    registerError,
    onLogout,
    logoutLoading,
    logoutError,
  };
};
