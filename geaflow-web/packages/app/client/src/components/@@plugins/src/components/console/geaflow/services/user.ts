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