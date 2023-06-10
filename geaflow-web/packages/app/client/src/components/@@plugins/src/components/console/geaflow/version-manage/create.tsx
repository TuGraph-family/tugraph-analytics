import React, { useState } from "react";
import { Modal, Form, Input, message, Upload } from "antd";
import { InboxOutlined } from "@ant-design/icons";
import { createVersion } from "../services/version";

interface CreateClusterProps {
  visible: boolean;
  reload: () => void;
  close: () => void;
}

const CreateVersion: React.FC<CreateClusterProps> = ({
  visible,
  close,
  reload,
}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    loading: false,
  });

  const handleCreateVersion = async () => {
    const values = await form.validateFields();

    setState({
      ...state,
      loading: true,
    });
    const { name, engineJarFile, comment } = values;

    const formData = new FormData();
    formData.append("name", name);

    formData.append("comment", comment);

    engineJarFile.forEach((item) => {
      formData.append("engineJarFile", item.originFileObj);
    });

    const params = {
      // name,
      engineJarFile: formData,
    };
    console.log(formData);
    const resp = await createVersion(formData);

    setState({
      ...state,
      loading: false,
    });
    if (resp?.success) {
      message.success("创建版本成功");
      reload();
    }
  };

  const normFile = (e: any) => {
    console.log("Upload event:", e);
    if (Array.isArray(e)) {
      return e;
    }
    return e?.fileList.slice(-1);
  };

  return (
    <Modal
      title="创建版本"
      width={700}
      visible={visible}
      onOk={handleCreateVersion}
      confirmLoading={state.loading}
      onCancel={close}
      okText="确认"
      cancelText="取消"
    >
      <Form form={form}>
        <Form.Item
          label="名称"
          name="name"
          rules={[{ required: true, message: "请输入版本名称" }]}
          initialValue=""
        >
          <Input />
        </Form.Item>
        <Form.Item label="版本描述" name="comment">
          <Input.TextArea rows={1} />
        </Form.Item>
        <Form.Item label="Jar文件">
          <Form.Item
            name="engineJarFile"
            valuePropName="fileList"
            getValueFromEvent={normFile}
            noStyle
          >
            <Upload.Dragger name="files">
              <p className="ant-upload-drag-icon">
                <InboxOutlined />
              </p>
              <p className="ant-upload-text">拖拽或点击选择文件</p>
              <p className="ant-upload-hint">只支持 jar 文件。</p>
            </Upload.Dragger>
          </Form.Item>
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default CreateVersion;
