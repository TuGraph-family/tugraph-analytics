import { Form, Input, message, Modal } from "antd";
import React, { useEffect, useState } from "react";
import type { AddInstanceProps } from "./interface";
import {
  createInstance,
  updateInstance,
  queryInstanceList,
} from "../services/instance";

const { TextArea } = Input;

export const AddInstanceModal: React.FC<AddInstanceProps> = ({
  isAddInstance,
  setIsAddInstance,
  itemtData,
  form,
}) => {
  console.log(itemtData);
  const [loading, setLoading] = useState(false);
  const queryInstance = async () => {
    let result: any = await queryInstanceList();
    if (result.code !== "SUCCESS") {
      message.error(`查询实例失败：${result.message}`);
      return;
    }

    const resultData = result.data?.list.filter(
      (item) =>
        item.id ===
        JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))?.key
    );

    if (resultData?.length > 0) {
      localStorage.setItem(
        "GEAFLOW_CURRENT_INSTANCE",
        JSON.stringify({
          key: resultData[0].id,
          value: resultData[0].name,
        })
      );
      form.resetFields();
      window.location.reload();
    }
  };
  const handleCreateInstance = async () => {
    const values = await form.validateFields();
    setLoading(true);
    const { name, comment } = values;
    const createParams = {
      name,
      comment,
    };

    let result = null;
    if (itemtData) {
      // 更新
      result = await updateInstance(createParams, itemtData?.name);
      if (result) {
        message.success("更新实例成功");
        queryInstance();
      }
    } else {
      result = await createInstance(createParams);
      if (result) {
        message.success("创建实例成功");
        queryInstance();
      }
    }
    setLoading(false);
    if (result) {
      setIsAddInstance(false);
    }
  };

  useEffect(() => {
    if (itemtData) {
      form.setFieldsValue(itemtData);
    }
  }, [itemtData]);

  return (
    <Modal
      title={itemtData ? "编辑实例" : "新增实例"}
      visible={isAddInstance}
      onOk={handleCreateInstance}
      onCancel={() => {
        setIsAddInstance(false);
        form.resetFields();
      }}
      confirmLoading={loading}
      okText="保存"
      cancelText="取消"
    >
      <Form.Item
        name="name"
        label="实例名称"
        rules={[{ required: true, message: "请输入实例名称" }]}
      >
        <Input placeholder="请输入实例名称" />
      </Form.Item>
      <Form.Item name="comment" label="描述">
        <TextArea rows={1} />
      </Form.Item>
    </Modal>
  );
};
