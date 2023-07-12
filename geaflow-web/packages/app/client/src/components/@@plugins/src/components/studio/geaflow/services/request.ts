import { extend } from 'umi-request'
import { notification } from "antd";
import $i18n from "../../../../../../i18n";

const codeMessage: Record<number, string> = {
  200: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheServerSuccessfullyReturnedThe",
    dm: "服务器成功返回请求的数据。",
  }),
  201: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheDataIsCreatedOr",
    dm: "新建或修改数据成功。",
  }),
  202: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.ARequestHasEnteredThe",
    dm: "一个请求已经进入后台排队（异步任务）。",
  }),
  204: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheDataWasDeleted",
    dm: "删除数据成功。",
  }),
  400: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.AnErrorOccurredInThe",
    dm: "发出的请求有错误，服务器没有进行新建或修改数据的操作。",
  }),
  401: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheUserDoesNotHave",
    dm: "用户没有权限（令牌、用户名、密码错误）。",
  }),
  403: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheUserIsAuthorizedBut",
    dm: "用户得到授权，但是访问是被禁止的。",
  }),
  404: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheRequestIsForRecords",
    dm: "发出的请求针对的是不存在的记录，服务器没有进行操作。",
  }),
  406: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheRequestFormatIsNot",
    dm: "请求的格式不可得。",
  }),
  410: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheRequestedResourceIsPermanently",
    dm: "请求的资源被永久删除，且不会再得到的。",
  }),
  422: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.WhenCreatingAnObjectA",
    dm: "当创建一个对象时，发生一个验证错误。",
  }),
  500: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.AnErrorOccurredOnThe",
    dm: "服务器发生错误，请检查服务器。",
  }),
  502: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.GatewayError",
    dm: "网关错误。",
  }),
  503: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheServiceIsUnavailableAnd",
    dm: "服务不可用，服务器暂时过载或维护。",
  }),
  504: $i18n.get({
    id: "openpiece-geaflow.geaflow.services.instance.TheGatewayTimedOut",
    dm: "网关超时。",
  }),
};

/** 异常处理程序 */
const errorHandler = (error: { response: Response }): Response => {
  const { response } = error;
  if (response && response.status) {
    const errorText = codeMessage[response.status] || response.statusText;
    const { status, url } = response;

    notification.error({
      message: $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.instance.RequestErrorStatusUrl",
          dm: "请求错误 {status}: {url}",
        },
        { status: status, url: url }
      ),
      description: errorText,
    });
  } else if (!response) {
    notification.error({
      description: $i18n.get({
        id: "openpiece-geaflow.geaflow.services.instance.YourServiceIsAbnormalAnd",
        dm: "您的服务发生异常，无法连接服务器",
      }),
      message: $i18n.get({
        id: "openpiece-geaflow.geaflow.services.instance.ServiceException",
        dm: "服务异常",
      }),
      key: "serverError",
      duration: null,
    });
  }
  return response;
};

const request = extend({
  errorHandler, // 默认错误处理
  credentials: "include", // 默认请求是否带上cookie
  headers: {
    'Accept-Language': localStorage.getItem('i18nextLng'),
    "geaflow-token": localStorage.getItem("GEAFLOW_TOKEN"),
  }
})

export default request
