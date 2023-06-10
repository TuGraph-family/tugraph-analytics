import React, { useEffect, useState, useRef } from "react";
import {
  Button,
  Modal,
  Popconfirm,
  message,
  Input,
  Space,
  Form,
  Select,
  Table,
} from "antd";
import {
  getUsers,
  deleteUser,
  getCreateUsers,
  updateUser,
} from "../services/use-manage";
import styles from "./list.module.less";
import { isEmpty } from "lodash";

const { Search } = Input;

export const UserManage: React.FC<{}> = ({}) => {
  const [temeplateList, setTemplateList] = useState({
    search: "",
    manageData: [],
    copyData: [],
    isAddMd: false,
    id: "",
  });
  const [form] = Form.useForm();
  const handelTemplata = async () => {
    const manageData = await getUsers({ name: temeplateList.search });
    setTemplateList({
      ...temeplateList,
      manageData,
      isAddMd: false,
      copyData: manageData,
    });
    form.resetFields();
  };

  useEffect(() => {
    handelTemplata();
  }, [temeplateList.search]);
  const handleCancel = () => {
    setTemplateList({ ...temeplateList, isAddMd: false });
    form.resetFields();
  };

  const columns = [
    {
      title: "用户名称",
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <span>{record.name || "-"}</span>

          <br />
          {record?.comment && (
            <span style={{ fontSize: 12, color: "#ccc" }}>
              {record.comment}
            </span>
          )}
        </span>
      ),
    },
    {
      title: "操作时间",
      key: "createTime",
      render: (_, record: any) => (
        <span>
          创建时间：{record.createTime} <br />
          {record?.modifyTime && <span>修改时间：{record.modifyTime}</span>}
        </span>
      ),
    },
    {
      title: "操作",
      key: "action",
      render: (_, record) => (
        <Popconfirm
          title="确认删除？"
          onConfirm={() => {
            deleteUser(record?.id).then((res) => {
              if (res?.success) {
                message.success("删除成功");
                handelTemplata();
              }
            });
          }}
          okText="确定"
          cancelText="取消"
        >
          <a>删除</a>
        </Popconfirm>
      ),
    },
  ];
  return (
    <div className={styles["user-manage"]}>
      <div className={styles["user-search"]}>
        <Search
          style={{ width: 286, marginRight: 16 }}
          placeholder="请输入搜索关键词"
          onSearch={(value) => {
            setTemplateList({ ...temeplateList, search: value });
          }}
        />
        <Button
          type="primary"
          onClick={() => {
            setTemplateList({
              ...temeplateList,
              isAddMd: true,
            });
          }}
        >
          添加
        </Button>
      </div>
      <Table
        dataSource={temeplateList.manageData}
        columns={columns}
        pagination={{
          hideOnSinglePage: true,
          showQuickJumper: true,
          size: "small",
        }}
      />
      <Modal
        title="添加用户"
        visible={temeplateList.isAddMd}
        onCancel={handleCancel}
        onOk={() => {
          form.validateFields().then((values) => {
            getCreateUsers(values).then((res) => {
              if (res?.success) {
                message.success("添加成功");
                handelTemplata();
              }
            });
          });
        }}
        okText="添加"
        cancelText="取消"
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="用户名称"
            rules={[{ required: true, message: "请输入用户名称" }]}
          >
            <Input />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};
