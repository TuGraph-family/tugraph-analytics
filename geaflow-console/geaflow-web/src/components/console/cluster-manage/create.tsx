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

import React, { useState } from "react";
import { Modal, Form, Input, message } from "antd";
import { useTranslation } from "react-i18next";
import { ClusterConfigPanel } from "./configPanel";
import { createCluster } from "../services/cluster";

interface CreateClusterProps {
  visible: boolean;
  reload: () => void;
  close: () => void;
}

const CreateCluster: React.FC<CreateClusterProps> = ({
  visible,
  reload,
  close,
}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    loading: false,
  });

  const { t } = useTranslation();

  const handleCreateCluster = async () => {
    const values = await form.validateFields();
    setState({
      ...state,
      loading: true,
    });
    const { name, comment, clusterConfig } = values;
    const { type, config } = clusterConfig;
    const configObj = {};
    for (const item of config) {
      configObj[item.key] = item.value;
    }

    const params = {
      name,
      comment,
      type,
      config: configObj,
    };

    const resp = await createCluster(params);

    setState({
      ...state,
      loading: false,
    });
    if (resp) {
      message.success(t("i18n.key.created.successfully"));
      reload();
    }
  };

  return (
    <Modal
      title={t("i18n.key.create.cluster")}
      width={780}
      visible={visible}
      onOk={handleCreateCluster}
      confirmLoading={state.loading}
      onCancel={() => {
        close();
      }}
      okText={t("i18n.key.confirm")}
      cancelText={t("i18n.key.cancel")}
    >
      <Form form={form}>
        <Form.Item
          label={t("i18n.key.name")}
          name="name"
          rules={[{ required: true, message: t("i18n.key.task.name") }]}
          initialValue=""
        >
          <Input />
        </Form.Item>
        <Form.Item label={t("i18n.key.cluster.description")} name="comment">
          <Input.TextArea rows={1} />
        </Form.Item>
        <ClusterConfigPanel
          prefixName="clusterConfig"
          values={{}}
          form={form}
        />
      </Form>
    </Modal>
  );
};

export default CreateCluster;
