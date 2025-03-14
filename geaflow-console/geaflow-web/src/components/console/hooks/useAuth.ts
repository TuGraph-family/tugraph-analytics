/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
