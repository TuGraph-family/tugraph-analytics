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
      title: "租户名称",
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
      title: "操作人",
      key: "creatorName",
      render: (_, record: any) => (
        <span>
          创建人：{record.creatorName} <br />
          {record?.modifierName && <span>修改人：{record.modifierName}</span>}
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
  ];
  return (
    <div className={styles["tenant-manage"]}>
      <div className={styles["tenant-search"]}>
        <Search
          style={{ width: 286, marginRight: 16 }}
          placeholder="请输入搜索关键词"
          onSearch={(value) => {
            setTemplateList({ ...temeplateList, search: value });
          }}
        />
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
