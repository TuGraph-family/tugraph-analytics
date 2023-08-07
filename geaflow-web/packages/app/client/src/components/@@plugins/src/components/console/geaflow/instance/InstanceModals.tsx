import { Form, Input, message, Modal } from "antd";
import React, { useEffect, useState } from "react";
import type { AddInstanceProps } from "./interface";
import {
  createInstance,
  updateInstance,
  queryInstanceList,
} from "../services/instance";
import $i18n from "../../../../../../i18n";

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
      message.error(
        $i18n.get(
          {
            id: "openpiece-geaflow.geaflow.instance.InstanceModals.FailedToQueryTheInstance",
            dm: "查询实例失败：{resultMessage}",
          },
          { resultMessage: result.message }
        )
      );
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
        message.success(
          $i18n.get({
            id: "openpiece-geaflow.geaflow.instance.InstanceModals.TheInstanceHasBeenUpdated",
            dm: "更新实例成功",
          })
        );
        queryInstance();
      }
    } else {
      result = await createInstance(createParams);
      if (result) {
        message.success(
          $i18n.get({
            id: "openpiece-geaflow.geaflow.instance.InstanceModals.TheInstanceIsCreated",
            dm: "创建实例成功",
          })
        );
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
      title={
        itemtData
          ? $i18n.get({
              id: "openpiece-geaflow.geaflow.instance.InstanceModals.EditAnInstance",
              dm: "编辑实例",
            })
          : $i18n.get({
              id: "openpiece-geaflow.geaflow.instance.InstanceModals.AddAnInstance",
              dm: "新增实例",
            })
      }
      visible={isAddInstance}
      onOk={handleCreateInstance}
      onCancel={() => {
        setIsAddInstance(false);
        form.resetFields();
      }}
      confirmLoading={loading}
      okText={$i18n.get({
        id: "openpiece-geaflow.geaflow.instance.InstanceModals.Save",
        dm: "保存",
      })}
      cancelText={$i18n.get({
        id: "openpiece-geaflow.geaflow.instance.InstanceModals.Cancel",
        dm: "取消",
      })}
    >
      <Form.Item
        name="name"
        label={$i18n.get({
          id: "openpiece-geaflow.geaflow.instance.InstanceModals.InstanceName",
          dm: "实例名称",
        })}
        rules={[
          {
            required: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.instance.InstanceModals.EnterAnInstanceName",
              dm: "请输入实例名称",
            }),
          },
        ]}
      >
        <Input
          placeholder={$i18n.get({
            id: "openpiece-geaflow.geaflow.instance.InstanceModals.EnterAnInstanceName",
            dm: "请输入实例名称",
          })}
        />
      </Form.Item>
      <Form.Item
        name="comment"
        label={$i18n.get({
          id: "openpiece-geaflow.geaflow.instance.InstanceModals.Description",
          dm: "描述",
        })}
      >
        <TextArea rows={1} />
      </Form.Item>
    </Modal>
  );
};
