import React, { useState } from "react";
import { Modal, Form, Input, message } from "antd";
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

  const handleCreateCluster = async () => {
    const values = await form.validateFields();
    console.log("创建", values);
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
      message.success("创建集群成功");
      reload();
    }
  };

  return (
    <Modal
      title="创建集群"
      width={700}
      visible={visible}
      onOk={handleCreateCluster}
      confirmLoading={state.loading}
      onCancel={() => {
        close();
      }}
      okText="确认"
      cancelText="取消"
    >
      <Form form={form}>
        <Form.Item
          label="名称"
          name="name"
          rules={[{ required: true, message: "请输入任务名称" }]}
          initialValue=""
        >
          <Input />
        </Form.Item>
        <Form.Item label="集群描述" name="comment">
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
