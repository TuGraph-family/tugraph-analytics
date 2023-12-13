import { isEmpty } from "lodash";
/**
 * @tags url
 * 获取 url params
 */
export function getUrlParam(param: string, location?: any) {
  const urlSearchParams = new URLSearchParams(
    !isEmpty(location) ? location.search : window.location.search
  );
  return urlSearchParams.get(param);
}
