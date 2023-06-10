import React, { useEffect, useState } from "react";
import { PlusOutlined } from "@ant-design/icons";
import { ProTable } from "@ant-design/pro-components";
import { Input, Modal, Button, Form, message, Space, Popconfirm } from "antd";
import { ClusterConfigPanel } from "./configPanel";
import styles from "./list.module.less";
import {
  getClustersList,
  updateClusters,
  deleteClusters,
} from "../services/cluster";
import CreateCluster from "./create";
import { isEmpty } from "lodash";

const { Search } = Input;

export const ClusterManage: React.FC<{}> = ({}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    list: [],
    originList: [],
    visible: false,
    item: {},
    name: "",
    search: "",
  });

  const [createState, setCreateState] = useState({
    visible: false,
  });
  const handelResources = async () => {
    const respData = await getClustersList({ name: state.search });
    setState({
      ...state,
      list: respData,
      originList: respData,
      visible: false,
    });
  };

  useEffect(() => {
    handelResources();
  }, [state.search]);

  const showCurrentClusterConfigInfo = (record) => {
    let defaultFormValues = {};
    // 根据 currentItem 来判断是新增还是修改
    const configArr = [];
    for (const key in record.config) {
      const current = record.config[key];
      configArr.push({
        key,
        value: current,
      });
    }
    defaultFormValues = {
      clusterConfig: {
        type: record.type,
        config: configArr,
      },
    };
    setState({
      ...state,
      visible: true,
      item: defaultFormValues,
      name: record.name,
    });
  };

  const columns = [
    {
      title: "集群名称",
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
      title: "集群名",
      dataIndex: "name",
      key: "name",
    },
    {
      title: "集群类型",
      dataIndex: "type",
      key: "type",
    },
    {
      title: "操作人",
      key: "creatorName",
      width: 150,
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
      width: 300,
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
            <a onClick={() => showCurrentClusterConfigInfo(record)}>编辑</a>
            <Popconfirm
              title="确认删除？"
              onConfirm={() => {
                deleteClusters(record?.name).then((res) => {
                  if (res?.success) {
                    message.success("删除成功");
                    handelResources();
                  }
                });
              }}
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
  const handleOkModal = () => {
    form.validateFields().then((values) => {
      const { clusterConfig = {} } = values;
      const { type, config = [] } = clusterConfig;
      const configObj = {
        type,
        config: {},
      };
      for (const item of config) {
        configObj.config[item.key] = item.value;
      }

      updateClusters(state.name, configObj).then((res) => {
        if (res?.code) {
          message.success("更新成功");
          handelResources();
        } else {
          message.error(res?.message);
        }
      });
    });
  };

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
        headerTitle="Geaflow集群"
        options={false}
        toolBarRender={() => [
          <Search
            placeholder="请输入搜索关键词"
            onSearch={(value) => {
              setState({
                ...state,
                search: value,
              });
            }}
            style={{ width: 250 }}
          />,
          <Button
            key="button"
            onClick={() => {
              setCreateState({
                ...createState,
                visible: true,
              });
            }}
            type="primary"
          >
            新增
          </Button>,
        ]}
      />
      <Modal
        title="集群配置参数"
        width={750}
        visible={state.visible}
        onCancel={handleCloseModal}
        onOk={handleOkModal}
        cancelText="取消"
        okText="确定"
      >
        <Form form={form}>
          <ClusterConfigPanel
            prefixName="clusterConfig"
            values={state.item}
            form={form}
          />
        </Form>
      </Modal>

      <CreateCluster
        visible={createState.visible}
        reload={() => {
          setCreateState({
            ...createState,
            visible: false,
          });
          handelResources();
        }}
        close={() => {
          setCreateState({
            ...createState,
            visible: false,
          });
        }}
      />
    </div>
  );
};
