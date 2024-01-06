import React from "react";
import { Collapse, FormInstance, Alert } from "antd";
import { useTranslation } from "react-i18next";
import { CommonConfig } from "./CommonConfig";
import { IDefaultValues } from "./index";
const { Panel } = Collapse;

interface IProps {
  values: {
    runtimeMetaConfig: IDefaultValues;
    haMetaConfig: IDefaultValues;
    metricConfig: IDefaultValues;
  };
  form: FormInstance;
}

export const RuntimeConfig: React.FC<IProps> = ({ values, form }) => {
  const { t } = useTranslation();
  return (
    <>
      <Alert
        message={t("i18n.key.runtime.meta.config.tips")}
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />
      <Collapse
        defaultActiveKey={["runtimeMetaConfig", "haMetaConfig", "metricConfig"]}
      >
        <Panel
          header={
            values.runtimeMetaConfig?.comment || t("i18n.key.runtime.metadata")
          }
          key="runtimeMetaConfig"
        >
          <CommonConfig
            prefixName="runtimeMetaConfig"
            values={values.runtimeMetaConfig}
            form={form}
          />
        </Panel>
        <Panel
          header={values.haMetaConfig?.comment || t("i18n.key.HA.storage")}
          key="haMetaConfig"
        >
          <CommonConfig
            prefixName="haMetaConfig"
            values={values.haMetaConfig}
            form={form}
          />
        </Panel>
        <Panel
          header={values.metricConfig?.comment || t("i18n.key.metric.Storage")}
          key="metricConfig"
        >
          <CommonConfig
            prefixName="metricConfig"
            values={values.metricConfig}
            form={form}
          />
        </Panel>
      </Collapse>
    </>
  );
};
