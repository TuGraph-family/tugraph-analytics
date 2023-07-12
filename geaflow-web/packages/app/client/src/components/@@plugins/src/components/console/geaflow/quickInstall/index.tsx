import React, { useEffect, useState } from "react";
import {
  LoadingOutlined,
  SolutionOutlined,
  UserOutlined,
} from "@ant-design/icons";
import { Steps, Button, message, Form, Spin, Alert } from "antd";
import { useOpenpieceUserAuth } from "@tugraph/openpiece-client";
import { useTranslation } from "react-i18next";
import { RuntimeConfig } from "./RuntimeConfig";
import { CommonConfig } from "./CommonConfig";
import {
  getQuickInstallParams,
  quickInstallInstance,
  switchUserRole,
} from "../services/quickInstall";
import { useImmer } from "use-immer";
import "./index.less";

export interface IDefaultValues {
  type: string;
  config: {};
  category: string;
  comment: string;
  name: string;
}

const transformDataToArr = (configData: any = {}) => {
  const dataConfigArr = [];
  for (const key in configData.config) {
    dataConfigArr.push({
      key,
      value: configData.config[key],
    });
  }
  const obj = {
    ...configData,
    config: dataConfigArr,
  };
  return obj;
};

interface RedirectPath {
  path: string;
  pathName: string;
}

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

export const QuickInstall: React.FC<PluginPorps> = ({ redirectPath }) => {
  const { switchRole, redirectLoginURL } = useOpenpieceUserAuth();
  const redirectUrl = redirectPath?.[0]?.path || "/";

  const { t } = useTranslation();

  const cacheInstallValues = JSON.parse(
    localStorage.getItem("QUICK_INSTALL_PARAMS")
  );

  const [current, setCurrent] = useState(0);
  const [loading, setLoading] = useState(false);
  const [deployMode, setDeployMode] = useState("LOCAL");
  const [configs, setConfigs] = useImmer({} as any);
  const [form] = Form.useForm();

  const defaultValues = {
    dataConfig: {
      config: [],
      category: "DATA",
      name: "data-store-default",
      comment: t("i18n.key.default.storage"),
    },
    runtimeClusterConfig: {
      config: [],
      category: "RUNTIME_CLUSTER",
      name: "cluster-default",
      comment: t("i18n.key.default.cluster"),
    },
    runtimeMetaConfig: {
      config: [],
      category: "RUNTIME_META",
      name: "runtime-meta-store-default",
      comment: t("i18n.key.runtime.storage"),
    },
    remoteFileConfig: {
      config: [],
      category: "REMOTE_FILE",
      name: "file-store-default",
      comment: t("i18n.key.file.storage"),
    },
    metricConfig: {
      config: [],
      category: "METRIC",
      name: "metric-store-default",
      comment: t("i18n.key.metric.storage"),
    },
    haMetaConfig: {
      config: [],
      category: "HA_META",
      name: "ha-meta-store-default",
      comment: t("i18n.key.metadata.storage"),
    },
  };

  const transformDataToObject = (key: string, arrData: any) => {
    const dataConfigObj = {};
    for (const d of arrData.config) {
      dataConfigObj[d.key] = d.type === "LONG" ? parseInt(d.value) : d.value;
    }
    const obj = {
      ...defaultValues[key],
      ...arrData,
      config: dataConfigObj,
    };
    return obj;
  };

  const getDefaultPluginCategoriesValue = async () => {
    const result = await getQuickInstallParams();
    if (result.code === "FORBIDDEN") {
      // 没有登陆，跳到登录页面
      redirectLoginURL();
      return;
    }

    if (result.code === "SUCCESS") {
      setDeployMode(result.data.deployMode);
      const {
        dataConfig,
        runtimeClusterConfig,
        runtimeMetaConfig,
        remoteFileConfig,
        metricConfig,
        haMetaConfig,
      } = result.data;
      const dataConfigData = transformDataToArr(dataConfig);
      const runtimeClusterConfigData = transformDataToArr(runtimeClusterConfig);
      const runtimeMetaConfigData = transformDataToArr(runtimeMetaConfig);
      const remoteFileConfigData = transformDataToArr(remoteFileConfig);
      const metricConfigData = transformDataToArr(metricConfig);
      const haMetaConfigData = transformDataToArr(haMetaConfig);

      if (!cacheInstallValues) {
        setConfigs((draft) => {
          draft.dataConfig = dataConfigData;
          draft.runtimeClusterConfig = runtimeClusterConfigData;
          draft.metricConfig = metricConfigData;
          draft.remoteFileConfig = remoteFileConfigData;
          draft.runtimeMetaConfig = runtimeMetaConfigData;
          draft.haMetaConfig = haMetaConfigData;
        });
      } else {
        setConfigs(cacheInstallValues || defaultValues);
      }
    }
  };

  useEffect(() => {
    getDefaultPluginCategoriesValue();
  }, []);

  if (!configs.runtimeClusterConfig) {
    return <Spin />;
  }

  const {
    dataConfig,
    runtimeClusterConfig,
    runtimeMetaConfig,
    remoteFileConfig,
    metricConfig,
    haMetaConfig,
  } = configs;

  const runtimeConfigData = {
    runtimeMetaConfig,
    haMetaConfig,
    metricConfig,
  };

  const steps = [
    {
      title: t("i18n.key.cluster.configuration"),
      status: "finish",
      icon: <UserOutlined />,
      content: (
        <CommonConfig
          prefixName="runtimeClusterConfig"
          values={runtimeClusterConfig}
          form={form}
          tipsInfo={t("i18n.key.cluster.config.tips")}
        />
      ),
    },
    {
      title: t("Runtime Configuration"),
      status: "finish",
      icon: <SolutionOutlined />,
      content: <RuntimeConfig values={runtimeConfigData} form={form} />,
    },
    {
      title: t("i18n.key.storage.configuration"),
      status: "process",
      icon: <LoadingOutlined />,
      content: (
        <CommonConfig
          prefixName="dataConfig"
          values={dataConfig}
          form={form}
          tipsInfo={t("i18n.key.data.store.config.tips")}
        />
      ),
    },
    {
      title: t("i18n.key.file.configuration"),
      status: "process",
      icon: <LoadingOutlined />,
      content: (
        <CommonConfig
          prefixName="remoteFileConfig"
          values={remoteFileConfig}
          form={form}
          tipsInfo={t("i18n.key.file.store.config.tips")}
        />
      ),
    },
  ];

  const next = async () => {
    const currentValues = await form.validateFields();

    // 点击下一步时候，缓存用户填写的值
    const cacheFormValues =
      JSON.parse(localStorage.getItem("QUICK_INSTALL_PARAMS")) || {};
    const cacheParams = {
      ...cacheFormValues,
      ...currentValues,
    };

    const hasExecQuickInstall = localStorage.getItem("HAS_EXEC_QUICK_INSTALL");

    // 如果没执行过一键安装，则缓存
    if (!hasExecQuickInstall) {
      // 将用户配置后端额数据进行缓存，刷新后依旧可使用
      localStorage.setItem("QUICK_INSTALL_PARAMS", JSON.stringify(cacheParams));
    }

    setConfigs({
      ...configs,
      ...currentValues,
    });
    setCurrent(current + 1);
  };

  const prev = () => {
    setCurrent(current - 1);
  };
  const items = steps.map((item) => ({ key: item.title, title: item.title }));

  const startInstall = async () => {
    setLoading(true);
    const currentValues = await form.validateFields();

    const cacheFormValues =
      JSON.parse(localStorage.getItem("QUICK_INSTALL_PARAMS")) || {};
    const cacheParams = {
      ...cacheFormValues,
      ...currentValues,
    };

    const hasExecQuickInstall = localStorage.getItem("HAS_EXEC_QUICK_INSTALL");

    // 如果没执行过一键安装，则缓存
    if (!hasExecQuickInstall) {
      // 将用户配置后端额数据进行缓存，刷新后依旧可使用
      localStorage.setItem("QUICK_INSTALL_PARAMS", JSON.stringify(cacheParams));
    }

    // 将所有参数中 config 由数组转成对象
    const { remoteFileConfig } = form.getFieldsValue();
    const {
      dataConfig,
      runtimeClusterConfig,
      runtimeMetaConfig,
      metricConfig,
      haMetaConfig,
    } = configs;

    const dataConfigDataObj = transformDataToObject("dataConfig", dataConfig);
    const runtimeClusterConfigDataObj = transformDataToObject(
      "runtimeClusterConfig",
      runtimeClusterConfig
    );
    const runtimeMetaConfigDataObj = transformDataToObject(
      "runtimeMetaConfig",
      runtimeMetaConfig
    );
    const remoteFileConfigDataObj = transformDataToObject(
      "remoteFileConfig",
      remoteFileConfig
    );
    const metricConfigDataObj = transformDataToObject(
      "metricConfig",
      metricConfig
    );
    const haMetaConfigDataObj = transformDataToObject(
      "haMetaConfig",
      haMetaConfig
    );

    const installParams = {
      dataConfig: dataConfigDataObj,
      runtimeClusterConfig: runtimeClusterConfigDataObj,
      runtimeMetaConfig: runtimeMetaConfigDataObj,
      remoteFileConfig: remoteFileConfigDataObj,
      metricConfig: metricConfigDataObj,
      haMetaConfig: haMetaConfigDataObj,
    };

    const result = await quickInstallInstance(installParams);
    setLoading(false);
    if (result.code !== "SUCCESS") {
      message.error(`${t("i18n.key.installation.failed")}：${result.message}`);
      return;
    }
    // 安装成功，则后端会切换角色，前端隐藏掉一键安装的页面
    message.success(t("i18n.key.installation.succeeded"));
    // 安装成功后删除缓存
    localStorage.removeItem("QUICK_INSTALL_PARAMS");

    // 安装成功以后，切换用户角色
    const resp = await switchUserRole();

    if (resp.code !== "SUCCESS") {
      // 切换用户角色失败
      message.error(`${t("i18n.key.switch.roles")}: ${resp.message}`);
      return;
    }

    // 一键安装并切换用户角色后，删除管理员标识
    localStorage.removeItem("IS_GEAFLOW_ADMIN");
    switchRole("member", redirectUrl);
  };

  // 是否执行过一键安装
  const hasExecQuickInstall = localStorage.getItem("HAS_EXEC_QUICK_INSTALL");

  return (
    <>
      <Alert
        message={
          deployMode === "CLUSTER"
            ? t("i18n.key.cluster.deploy.tips")
            : t("i18n.key.local.deploy.tips")
        }
        type="info"
        style={{ marginBottom: 24 }}
      />
      <Form form={form} initialValues={configs}>
        {/* <Steps current={current} items={items} /> */}
        <Steps current={current}>
          {steps.map((item) => {
            return <Steps.Step title={item.title}>{item.content}</Steps.Step>;
          })}
        </Steps>
        <div className="steps-content">{steps[current].content}</div>
        <div className="steps-action">
          {current > 0 && (
            <Button style={{ margin: "0 8px" }} onClick={() => prev()}>
              {t("i18n.key.previous")}
            </Button>
          )}
          {current < steps.length - 1 && (
            <Button type="primary" onClick={() => next()}>
              {t("i18n.key.next")}
            </Button>
          )}
          {current === steps.length - 1 && !hasExecQuickInstall && (
            <Button type="primary" onClick={startInstall} loading={loading}>
              {t("i18n.key.install")}
            </Button>
          )}
        </div>
      </Form>
    </>
  );
};
