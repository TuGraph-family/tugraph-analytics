import React, { useEffect, useState } from "react";
import { ProTable } from "@ant-design/pro-components";
import { PlusOutlined } from "@ant-design/icons";
import {
  Input,
  Modal,
  Tag,
  Descriptions,
  Button,
  Space,
  message,
  Popconfirm,
} from "antd";
import { deleteVersion, getVersionList } from "../services/version";
import CreateVersion from "./create";
import styles from "./list.module.less";

const { Search } = Input;

export const VersionManage: React.FC<{}> = ({}) => {
  const [state, setState] = useState({
    list: [],
    originList: [],
    visible: false,
    item: {},
    createVisible: false,
    reloadCount: 0,
    Search: "",
    comment: "",
  });
  const handelResources = async () => {
    const respData = await getVersionList({ name: state.Search });
    setState({
      ...state,
      list: respData,
      originList: respData,
    });
  };

  useEffect(() => {
    handelResources();
  }, [state.reloadCount, state.Search]);

  const showCurrentClusterConfigInfo = (record) => {
    console.log(record);
    setState({
      ...state,
      visible: true,
      item: record.engineJarPackage,
      comment: record?.comment,
    });
  };

  const handleDelete = async (record) => {
    const resp = await deleteVersion(record.name);
    if (resp) {
      message.success("删除成功");
      setState({
        ...state,
        reloadCount: ++state.reloadCount,
      });
    }
  };

  const columns = [
    {
      title: "版本名称",
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
      title: "状态",
      dataIndex: "type",
      key: "type",
      render: (text) => {
        return text ? (
          <Tag color="green">已发布</Tag>
        ) : (
          <Tag color="red">未发布</Tag>
        );
      },
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

  const handleCloseModal = () => {
    setState({
      ...state,
      visible: false,
      item: {},
    });
  };

  const { item } = state;
  console.log(item);
  return (
    <div className={styles["example-table"]}>
      <ProTable
        columns={columns}
        search={false}
        cardBordered
        scroll={{ x: 1000 }}
        dataSource={state.list}
        rowKey="key"
        pagination={{
          showQuickJumper: true,
          hideOnSinglePage: true,
        }}
        dateFormatter="string"
        options={false}
        toolBarRender={() => [
          <Search
            placeholder="请输入搜索关键词"
            onSearch={(value) => {
              setState({
                ...state,
                Search: value,
              });
            }}
            style={{ width: 250 }}
          />,
          <Button
            key="button"
            onClick={() => {
              setState({
                ...state,
                createVisible: true,
              });
            }}
            type="primary"
          >
            新增
          </Button>,
        ]}
      />
      <Modal
        title="版本配置参数"
        width={850}
        visible={state.visible}
        onCancel={handleCloseModal}
        onOk={handleCloseModal}
        okText="确定"
      >
        <Descriptions column={2}>
          <Descriptions.Item label="JAR包名称">{item.name}</Descriptions.Item>
          <Descriptions.Item label="描述">{state.comment}</Descriptions.Item>
          <Descriptions.Item label="类型">{item.type}</Descriptions.Item>
          <Descriptions.Item label="MD5">{item.md5}</Descriptions.Item>
          <Descriptions.Item label="路径">{item.path}</Descriptions.Item>
          <Descriptions.Item label="URL">{item.url}</Descriptions.Item>
          <Descriptions.Item label="创建人ID">
            {item.creatorId}
          </Descriptions.Item>
          <Descriptions.Item label="创建时间">
            {item.createTime}
          </Descriptions.Item>
          <Descriptions.Item label="更新时间">
            {item.modifyTime}
          </Descriptions.Item>
        </Descriptions>
      </Modal>

      <CreateVersion
        visible={state.createVisible}
        close={() =>
          setState({
            ...state,
            createVisible: false,
          })
        }
        reload={() =>
          setState({
            ...state,
            createVisible: false,
            reloadCount: ++state.reloadCount,
          })
        }
      />
    </div>
  );
};
