import React, { useEffect, useState } from "react";
import {
  Input,
  Modal,
  Tag,
  Table,
  Button,
  Space,
  message,
  Popconfirm,
  Descriptions,
} from "antd";
import {
  deleteFunction,
  getFunctions,
  getRemoteFiles,
} from "../services/function-manage";
import { AddTemplateModal } from "./uploadModal";

const { Search } = Input;

export const GeaflowFunctionManage: React.FC<{}> = ({}) => {
  const [state, setState] = useState({
    functionManage: [],
    search: "",
    isAddMd: false,
    files: [],
    visible: false,
    item: {},
  });
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const instanceName = currentInstance.value;
  const handelResources = async () => {
    if (instanceName) {
      const respData = await getFunctions(instanceName, state.search);
      const files = await getRemoteFiles();
      setState({
        ...state,
        functionManage: respData,
        files,
        isAddMd: false,
      });
    }
  };

  useEffect(() => {
    handelResources();
  }, [state.search, instanceName]);

  const handleDelete = async (record) => {
    const resp = await deleteFunction(instanceName, record.name);
    if (resp) {
      message.success("删除成功");
      handelResources();
    }
  };
  const showCurrentClusterConfigInfo = (record) => {
    console.log(record);
    setState({
      ...state,
      visible: true,
      item: record.jarPackage,
    });
  };

  const columns = [
    {
      title: "函数名称",
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <a onClick={() => showCurrentClusterConfigInfo(record)}>
            {record.name}
          </a>
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
      title: "EntryClass",
      dataIndex: "entryClass",
      key: "entryClass",
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
      dataIndex: "container",
      key: "container",
      hideInSearch: true,
      render: (_, record) => {
        return (
          <Space>
            <Popconfirm
              title="确定删除该记录吗？"
              onConfirm={() => handleDelete(record)}
              okText="确定"
              cancelText="取消"
            >
              <a>删除</a>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];
  const { item } = state;
  return (
    <div style={{ padding: 16, background: "#fff" }}>
      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          marginBottom: 16,
        }}
      >
        <Search
          style={{ width: 286, marginRight: 16 }}
          placeholder="请输入搜索关键词"
          onSearch={(value) => {
            setState({ ...state, search: value });
          }}
        />
        <Button
          onClick={() => {
            setState({
              ...state,
              isAddMd: true,
            });
          }}
          type="primary"
        >
          新增
        </Button>
      </div>
      <Table
        dataSource={state.functionManage}
        columns={columns}
        pagination={{
          hideOnSinglePage: true,
          showQuickJumper: true,
          size: "small",
        }}
      />
      <AddTemplateModal
        isAddMd={state.isAddMd}
        onclose={() => {
          setState({
            ...state,
            isAddMd: false,
          });
        }}
        onLoad={() => {
          handelResources();
        }}
        instanceName={instanceName}
        files={state.files}
      />
      <Modal
        title="函数配置参数"
        width={850}
        visible={state.visible}
        onCancel={() => {
          setState({ ...state, visible: false });
        }}
        onOk={() => {
          setState({ ...state, visible: false });
        }}
        okText="确定"
      >
        <Descriptions column={2}>
          <Descriptions.Item label="JAR包名称">{item?.name}</Descriptions.Item>
          <Descriptions.Item label="类型">{item?.type}</Descriptions.Item>
          <Descriptions.Item label="MD5">{item?.md5}</Descriptions.Item>
          <Descriptions.Item label="路径">{item?.path}</Descriptions.Item>
          <Descriptions.Item label="URL">{item?.url}</Descriptions.Item>
          <Descriptions.Item label="创建人ID">
            {item?.creatorId}
          </Descriptions.Item>
          <Descriptions.Item label="创建时间">
            {item?.createTime}
          </Descriptions.Item>
          <Descriptions.Item label="更新时间">
            {item?.modifyTime}
          </Descriptions.Item>
        </Descriptions>
      </Modal>
    </div>
  );
};
