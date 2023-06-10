import React, { useEffect, useState, useRef } from "react";
import { Button, message, Input } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import type { ActionType } from "@ant-design/pro-components";
import { ProTable } from "@ant-design/pro-components";
import { queryInstanceList } from "../services/instance";
import { AddInstanceModal } from "./InstanceModals";
import type { InstanceTableProps } from "./interface";
import { isEmpty } from "lodash";
import styles from "./list.module.less";

const { Search } = Input;

export const InstanceTable: React.FC<InstanceTableProps> = ({ form }) => {
  const [isEdit, setIsEdit] = useState<boolean>(false);
  const [itemData, setItemData] = useState(null);
  const [instanceData, setInstanceData] = useState({
    list: [],
    // 用于前端筛选
    originList: [],
    name: "",
    creatorId: "",
  });

  const actionRef = useRef<ActionType>();

  const getInstanceList = async () => {
    let result: any = await queryInstanceList({
      name: instanceData.name,
    });

    if (result.code !== "SUCCESS") {
      message.error(`查询实例失败：${result.message}`);
      return;
    }

    setInstanceData({
      ...instanceData,
      list: result.data?.list,
      originList: result.data?.list,
    });
  };

  const showCreateModal = () => {
    setIsEdit(true);
    setItemData(null);
  };

  useEffect(() => {
    getInstanceList();
  }, [instanceData.name]);

  const columns = [
    {
      title: "实例名称",
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <span>{record.name}</span>

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
    {
      title: "操作",
      key: "action",
      hideInSearch: true,
      render: (_, record) => {
        return (
          <a
            onClick={() => {
              setIsEdit(true);
              setItemData(record);
            }}
          >
            编辑
          </a>
        );
      },
    },
  ];
  return (
    <div className={styles["example-table"]}>
      <ProTable
        columns={columns}
        actionRef={actionRef}
        cardBordered
        dataSource={instanceData.list}
        rowKey="id"
        search={false}
        scroll={{ x: 1000 }}
        pagination={{
          showQuickJumper: true,
          hideOnSinglePage: true,
        }}
        dateFormatter="string"
        options={false}
        toolBarRender={() => [
          <p
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <Search
              style={{ width: 286, marginRight: 16 }}
              placeholder="请输入搜索关键词"
              onSearch={(value) => {
                setInstanceData({ ...instanceData, name: value });
              }}
            />
            <Button key="button" type="primary" onClick={showCreateModal}>
              新增
            </Button>
          </p>,
        ]}
      />

      {/* 编辑 */}
      <AddInstanceModal
        setIsAddInstance={setIsEdit}
        isAddInstance={isEdit}
        itemtData={itemData}
        form={form}
      />
    </div>
  );
};
