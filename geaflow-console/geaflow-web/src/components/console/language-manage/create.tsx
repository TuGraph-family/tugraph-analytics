import React, { useState, useEffect } from "react";
import { Modal, Form, Input, message, Select } from "antd";
import { useTranslation } from "react-i18next";
import {
  createLanguage,
  UpdateLanguage,
  getTypes,
} from "../services/languages";
import { isEmpty } from "lodash";

interface CreateClusterProps {
  visible: boolean;
  reload: () => void;
  close: () => void;
  editValue?: object;
}

const { TextArea } = Input;

const CreateCluster: React.FC<CreateClusterProps> = ({
  visible,
  reload,
  close,
  editValue,
}) => {
  const [form] = Form.useForm();
  const typeName = Form.useWatch("type", form);
  const [state, setState] = useState({
    loading: false,
    types: [],
  });
  const { t } = useTranslation();

  const handleCreateCluster = () => {
    form.validateFields().then((values) => {
      setState({
        ...state,
        loading: true,
      });
      if (!isEmpty(editValue)) {
        UpdateLanguage(values, editValue.name).then((res) => {
          setState({
            ...state,
            loading: false,
          });
          if (res?.success) {
            message.success(t("i18n.key.edit.language.successfully"));
            reload();
            form.resetFields();
          }
        });
      } else {
        createLanguage(values).then((res) => {
          setState({
            ...state,
            loading: false,
          });
          if (res) {
            message.success(t("i18n.key.created.language.successfully"));
            reload();
            form.resetFields();
          }
        });
      }
    });
  };

  useEffect(() => {
    if (!isEmpty(editValue)) {
      form.setFieldsValue(editValue);
    }

    getTypes().then((res) => {
      setState({
        ...state,
        types: res,
      });
      if (isEmpty(editValue)) {
        form.setFieldsValue({
          type: res[0],
        });
      }
    });
  }, [editValue]);

  useEffect(() => {
    if (isEmpty(editValue)) {
      if (typeName === "OPEN_AI") {
        form.setFieldsValue({
          url: "https://api.openai.com/v1/chat/completions",
        });
      } else {
        form.setFieldsValue({
          url: "",
        });
      }
    }
  }, [typeName, editValue]);

  return (
    <Modal
      title={t("i18n.key.create.language")}
      width={780}
      open={visible}
      onOk={handleCreateCluster}
      confirmLoading={state.loading}
      onCancel={() => {
        close();
        form.resetFields();
      }}
      okText={t("i18n.key.confirm")}
      cancelText={t("i18n.key.cancel")}
    >
      <Form form={form}>
        <Form.Item
          label={t("i18n.key.name")}
          name="name"
          rules={[
            { required: true, message: t("i18n.key.please.language.name") },
          ]}
          initialValue=""
        >
          <Input />
        </Form.Item>
        <Form.Item label={t("i18n.key.comment")} name="comment">
          <Input.TextArea rows={1} />
        </Form.Item>
        <Form.Item
          label={t("i18n.key.type")}
          name="type"
          rules={[{ required: true, message: t("i18n.key.select.type") }]}
          initialValue={""}
        >
          <Select disabled={!isEmpty(editValue)}>
            {state.types?.map((item) => {
              return (
                <Select.Option value={item} key={item}>
                  {item}
                </Select.Option>
              );
            })}
          </Select>
        </Form.Item>
        <Form.Item
          label={t("i18n.key.url")}
          name="url"
          rules={[{ required: true, message: t("i18n.key.language.url") }]}
          initialValue=""
        >
          <Input />
        </Form.Item>
        <Form.Item label={t("i18n.key.args")} name="args" initialValue="">
          <TextArea rows={6} />
        </Form.Item>
        {/*
        <Form.Item
          label={"accessId"}
          name="accessId"
          rules={[{ required: true, message: t("i18n.key.language.accessId") }]}
          initialValue=""
        >
          <Input />
        </Form.Item>
        <Form.Item
          label={"accessKey"}
          name="accessKey"
          rules={[
            { required: true, message: t("i18n.key.language.accessKey") },
          ]}
          initialValue=""
        >
          <Input />
        </Form.Item> */}
      </Form>
    </Modal>
  );
};

export default CreateCluster;
