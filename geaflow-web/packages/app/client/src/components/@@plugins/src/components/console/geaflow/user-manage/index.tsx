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
import $i18n from "../../../../../../i18n";

const { Search } = Input;

export const UserManage: React.FC<{}> = ({}) => {
  const [temeplateList, setTemplateList] = useState({
    search: "",
    manageData: [],
    copyData: [],
    isAddMd: false,
    id: "",
  });
  const isAdminLogin = localStorage.getItem("IS_ADMIN_LOGIN");
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.user-manage.UserName",
        dm: "用户名称",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.user-manage.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.user-manage.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.user-manage.ModificationTimeRecordmodifytime",
                  dm: "修改时间：{recordModifyTime}",
                },
                { recordModifyTime: record.modifyTime }
              )}
            </span>
          )}
        </span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.user-manage.Operation",
        dm: "操作",
      }),
      key: "action",
      render: (_, record) => (
        <Popconfirm
          title={$i18n.get({
            id: "openpiece-geaflow.geaflow.user-manage.AreYouSureYouWant",
            dm: "确认删除？",
          })}
          onConfirm={() => {
            deleteUser(record?.id).then((res) => {
              if (res?.success) {
                message.success(
                  $i18n.get({
                    id: "openpiece-geaflow.geaflow.user-manage.DeletedSuccessfully",
                    dm: "删除成功",
                  })
                );
                handelTemplata();
              }
            });
          }}
          okText={$i18n.get({
            id: "openpiece-geaflow.geaflow.user-manage.Ok",
            dm: "确定",
          })}
          cancelText={$i18n.get({
            id: "openpiece-geaflow.geaflow.user-manage.Cancel",
            dm: "取消",
          })}
        >
          <a>
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.user-manage.Delete",
              dm: "删除",
            })}
          </a>
        </Popconfirm>
      ),
    },
  ];

  return (
    <div className={styles["user-manage"]}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <div style={{ fontWeight: 500, fontSize: 16 }}>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.user-manage.UsersList",
            dm: "用户列表",
          })}
        </div>
        <div>
          <Search
            style={{ width: 286, marginRight: 16 }}
            placeholder={$i18n.get({
              id: "openpiece-geaflow.geaflow.user-manage.EnterASearchKeyword",
              dm: "请输入搜索关键词",
            })}
            onSearch={(value) => {
              setTemplateList({ ...temeplateList, search: value });
            }}
          />

          {isAdminLogin && (
            <Button
              type="primary"
              onClick={() => {
                setTemplateList({
                  ...temeplateList,
                  isAddMd: true,
                });
              }}
            >
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.user-manage.Add",
                dm: "添加",
              })}
            </Button>
          )}
        </div>
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
        title={$i18n.get({
          id: "openpiece-geaflow.geaflow.user-manage.AddUser",
          dm: "添加用户",
        })}
        visible={temeplateList.isAddMd}
        onCancel={handleCancel}
        onOk={() => {
          form.validateFields().then((values) => {
            getCreateUsers(values).then((res) => {
              if (res?.success) {
                message.success(
                  $i18n.get({
                    id: "openpiece-geaflow.geaflow.user-manage.AddedSuccessfully",
                    dm: "添加成功",
                  })
                );
                handelTemplata();
              }
            });
          });
        }}
        okText={$i18n.get({
          id: "openpiece-geaflow.geaflow.user-manage.Add",
          dm: "添加",
        })}
        cancelText={$i18n.get({
          id: "openpiece-geaflow.geaflow.user-manage.Cancel",
          dm: "取消",
        })}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label={$i18n.get({
              id: "openpiece-geaflow.geaflow.user-manage.UserName",
              dm: "用户名称",
            })}
            rules={[
              {
                required: true,
                message: $i18n.get({
                  id: "openpiece-geaflow.geaflow.user-manage.EnterAUserName",
                  dm: "请输入用户名称",
                }),
              },
            ]}
          >
            <Input />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};
