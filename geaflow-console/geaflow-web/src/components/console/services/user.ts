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

import request from "./request";
import { HTTP_SERVICE_URL } from '../constants'

/**
 * 根据关键词匹配用户
 * @param keyword 搜索关键词
 */
export const autoCompleteUser = async (keyword: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/user/keyword`, {
    method: "get",
    params: {
      keyword,
    },
  });

  if (!response.success) {
    // message.error(`新增实例失败: ${response.message}`);
    return null;
  }
  return response.data;
};