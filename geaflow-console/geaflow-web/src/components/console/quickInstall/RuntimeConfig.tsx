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
