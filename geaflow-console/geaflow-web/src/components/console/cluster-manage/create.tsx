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
