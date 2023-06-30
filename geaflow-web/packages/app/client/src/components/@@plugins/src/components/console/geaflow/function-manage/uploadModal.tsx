import { Modal, Form, Input, message, Upload, Select, Radio } from "antd";
import { UploadOutlined } from "@ant-design/icons";
import React, { useEffect, useState } from "react";
import { createFunction, getRemoteFiles } from "../services/function-manage";
import { InboxOutlined } from "@ant-design/icons";
import { isEmpty } from "lodash";

const { Dragger } = Upload;
interface AddTemplateProps {
  isAddMd?: boolean;
  instanceName?: string;
  onLoad?: () => void;
  onclose?: () => void;
  files?: any;
}

export const AddTemplateModal: React.FC<AddTemplateProps> = ({
  isAddMd,
  onclose,
  instanceName,
  onLoad,
  files,
}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    loading: false,
    fileList: [],
    value: 1,
  });
  useEffect(() => {}, []);
  const handleCancel = () => {
    onclose();
    form.resetFields();
  };
  const handleOk = async () => {
    const values = await form.validateFields();
    setState({
      ...state,
      loading: true,
    });
    const { functionFile, name, type, entryClass, fileId, comment } = values;

    const formData = new FormData();

    !isEmpty(functionFile) &&
      functionFile.forEach((item) => {
        formData.append("functionFile", item.originFileObj);
      });
    formData.append("name", name);
    formData.append("type", type);
    formData.append("entryClass", entryClass);
    fileId && formData.append("fileId", fileId);
    comment && formData.append("comment", comment);
    const resp = await createFunction(instanceName, formData);

    setState({
      ...state,
      loading: false,
    });
    if (resp?.success) {
      message.success("上传成功");
      onLoad();
      form.resetFields();
    } else {
      message.error(resp?.message);
    }
  };

  const normFile = (e: any) => {
    if (Array.isArray(e)) {
      return e;
    }
    setState({ ...state, fileList: e?.fileList });
    return e?.fileList.slice(-1);
  };

  const props = {
    beforeUpload: (file) => {
      const isJar = file.type === "application/java-archive";
      if (!isJar) {
        message.error(`${file.name} 不是jar文件类型!`);
        return Upload.LIST_IGNORE;
      }
      return false;
    },
  };

  return (
    <Modal
      title="上传文件"
      visible={isAddMd}
      onOk={handleOk}
      onCancel={handleCancel}
      confirmLoading={state.loading}
      width={700}
      okText="确认"
      cancelText="取消"
    >
      <Form form={form}>
        <Form.Item
          name="name"
          label="名称"
          rules={[{ required: true, message: "请输入用户名称" }]}
        >
          <Input placeHolder="请输入用户名称" />
        </Form.Item>
        <Form.Item
          name="type"
          label="类型"
          rules={[{ required: true, message: "请选择类型" }]}
          initialValue={"UDF"}
        >
          <Select
            placeholder="请选择类型"
            options={[
              {
                value: "UDF",
                label: "UDF",
              },
              {
                value: "UDTF",
                label: "UDTF",
              },
              {
                value: "UDAF",
                label: "UDAF",
              },
            ]}
          />
        </Form.Item>
        <Form.Item
          name="entryClass"
          label="主函数"
          rules={[{ required: true, message: "请输入主函数" }]}
        >
          <Input placeHolder="请输入主函数" />
        </Form.Item>
        <Form.Item name="comment" label="描述">
          <Input.TextArea placeHolder="请输入描述" />
        </Form.Item>
        <Form.Item name="radio" initialValue={state.value}>
          <Radio.Group
            onChange={(e) => {
              setState({ ...state, value: e.target.value });
            }}
            value={state.value}
          >
            <Radio value={1}>上传文件</Radio>
            <Radio value={2}>绑定已有文件</Radio>
          </Radio.Group>
        </Form.Item>
        {state.value === 1 ? (
          <Form.Item label="Jar文件">
            <Form.Item
              name="functionFile"
              valuePropName="fileList"
              getValueFromEvent={normFile}
              noStyle
            >
              <Upload.Dragger {...props}>
                <p className="ant-upload-drag-icon">
                  <InboxOutlined />
                </p>
                <p className="ant-upload-text">拖拽或点击选择文件</p>
                <p className="ant-upload-hint">只支持 jar 文件。</p>
              </Upload.Dragger>
            </Form.Item>
          </Form.Item>
        ) : (
          <Form.Item name="fileId" label="已有文件">
            <Select
              placeholder="请选择已有文件"
              allowClear={true}
              options={files || []}
              fieldNames={{
                label: "name",
                value: "id",
              }}
            />
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
};
