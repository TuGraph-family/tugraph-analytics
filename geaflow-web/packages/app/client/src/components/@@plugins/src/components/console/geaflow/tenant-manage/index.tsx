import React, { useEffect, useState, useRef } from "react";
import {
  Button,
  Modal,
  Popconfirm,
  message,
  Input,
  Space,
  Form,
  Table,
} from "antd";
import { ProTable } from "@ant-design/pro-components";
import { getTenantList } from "../services/tenant-manage";
import { isEmpty } from "lodash";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

const { Search } = Input;

export const TenantManage: React.FC<{}> = ({}) => {
  const [temeplateList, setTemplateList] = useState({
    search: "",
    manageData: [],
    copyData: [],
  });

  const handelTemplata = async () => {
    const manageData = await getTenantList({ name: temeplateList.search });
    setTemplateList({ ...temeplateList, manageData, copyData: manageData });
  };

  useEffect(() => {
    handelTemplata();
  }, [temeplateList.search]);

  const columns = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.tenant-manage.TenantName",
        dm: "租户名称",
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
        id: "openpiece-geaflow.geaflow.tenant-manage.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.tenant-manage.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.tenant-manage.ModifiedByRecordmodifiername",
                  dm: "修改人：{recordModifierName}",
                },
                { recordModifierName: record.modifierName }
              )}
            </span>
          )}
        </span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.tenant-manage.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.tenant-manage.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.tenant-manage.ModificationTimeRecordmodifytime",
                  dm: "修改时间：{recordModifyTime}",
                },
                { recordModifyTime: record.modifyTime }
              )}
            </span>
          )}
        </span>
      ),
    },
  ];
  return (
    <div className={styles["tenant-manage"]}>
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
            id: "openpiece-geaflow.geaflow.tenant-manage.TenantsList",
            dm: "租户列表",
          })}
        </div>
        <div>
          <Search
            style={{ width: 286, marginRight: 16 }}
            placeholder={$i18n.get({
              id: "openpiece-geaflow.geaflow.tenant-manage.EnterASearchKeyword",
              dm: "请输入搜索关键词",
            })}
            onSearch={(value) => {
              setTemplateList({ ...temeplateList, search: value });
            }}
          />
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
    </div>
  );
};
