import React, { useEffect, useState, useRef } from "react";
import { Button, message, Input, Table } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import type { ActionType } from "@ant-design/pro-components";
import { ProTable } from "@ant-design/pro-components";
import { queryInstanceList } from "../services/instance";
import { AddInstanceModal } from "./InstanceModals";
import type { InstanceTableProps } from "./interface";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

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
      message.error(
        $i18n.get(
          {
            id: "openpiece-geaflow.geaflow.instance.InstanceTable.FailedToQueryTheInstance",
            dm: "查询实例失败：{resultMessage}",
          },
          { resultMessage: result.message }
        )
      );
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.instance.InstanceTable.InstanceName",
        dm: "实例名称",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.instance.InstanceTable.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.instance.InstanceTable.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.instance.InstanceTable.ModifiedByRecordmodifiername",
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
        id: "openpiece-geaflow.geaflow.instance.InstanceTable.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.instance.InstanceTable.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.instance.InstanceTable.ModificationTimeRecordmodifytime",
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
        id: "openpiece-geaflow.geaflow.instance.InstanceTable.Operation",
        dm: "操作",
      }),
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
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.instance.InstanceTable.Edit",
              dm: "编辑",
            })}
          </a>
        );
      },
    },
  ];

  return (
    <div className={styles["example-table"]}>
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
            id: "openpiece-geaflow.geaflow.instance.InstanceTable.InstancesList",
            dm: "实例列表",
          })}
        </div>
        <div>
          <Search
            style={{ width: 286, marginRight: 16 }}
            placeholder={$i18n.get({
              id: "openpiece-geaflow.geaflow.instance.InstanceTable.EnterASearchKeyword",
              dm: "请输入搜索关键词",
            })}
            onSearch={(value) => {
              setInstanceData({ ...instanceData, name: value });
            }}
          />

          <Button key="button" type="primary" onClick={showCreateModal}>
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.instance.InstancesTable.Add",
              dm: "新增",
            })}
          </Button>
        </div>
      </div>
      <Table
        columns={columns}
        dataSource={instanceData.list}
        pagination={{
          hideOnSinglePage: true,
          showQuickJumper: true,
          size: "small",
        }}
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
