import React, { useEffect, useState } from "react";
import {
  Input,
  Button,
  Table,
  Breadcrumb,
  Space,
  Popconfirm,
  message,
} from "antd";
import {
  getTableDefinitionList,
  deleteTableDefinition,
} from "../services/tableDefinition";
import CreateTableDefinition from "./create";
import styles from "./list.module.less";

const { Search } = Input;

export const TableDefinition: React.FC<{}> = ({}) => {
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  // TODO
  const instanceName = currentInstance.value;

  const [state, setState] = useState({
    list: [],
    originList: [],
    search: "",
  });

  const [showCreatePage, setShowCreatePage] = useState({
    visible: false,
    currentItem: null,
    realodedList: false,
    readonly: false,
  });

  const handelTemplata = async () => {
    const manageData = await getTableDefinitionList({
      instanceName,
      name: state.search,
    });

    setState({
      ...state,
      list: manageData,
      originList: manageData,
    });
  };

  useEffect(() => {
    // 只有当实例存在时才查询
    if (instanceName) {
      handelTemplata();
    }
  }, [instanceName, showCreatePage.realodedList, state.search]);

  const columns = [
    {
      title: "表名称",
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <a
            onClick={() =>
              setShowCreatePage({
                visible: true,
                currentItem: record,
                realodedList: false,
                readonly: true,
              })
            }
          >
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
      render: (_, record) => (
        <Space>
          <a
            onClick={() =>
              setShowCreatePage({
                visible: true,
                currentItem: record,
                realodedList: false,
                readonly: false,
              })
            }
          >
            编辑
          </a>
          <Popconfirm
            title="确认删除？"
            onConfirm={() => {
              deleteTableDefinition(instanceName, record?.name).then((res) => {
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
        </Space>
      ),
    },
  ];

  const handleSearch = (value) => {
    setState({
      ...state,
      search: value,
    });
  };

  return (
    <div className={styles["graph-definition"]}>
      {showCreatePage.visible ? (
        <>
          <Breadcrumb>
            <Breadcrumb.Item>
              <a
                onClick={() =>
                  setShowCreatePage({
                    visible: false,
                    currentItem: null,
                    realodedList: false,
                    readonly: false,
                  })
                }
              >
                表定义
              </a>
            </Breadcrumb.Item>

            <Breadcrumb.Item>
              {showCreatePage.readonly
                ? "表定义详情"
                : showCreatePage.currentItem
                ? "编辑表定义"
                : "新增表定义"}
            </Breadcrumb.Item>
          </Breadcrumb>
          <CreateTableDefinition
            currentItem={showCreatePage.currentItem}
            toBackList={setShowCreatePage}
            readonly={showCreatePage.readonly}
          />
        </>
      ) : (
        <div className={styles["definition"]}>
          <p>
            <span className={styles["definition-title"]}>表定义</span>
            <span className={styles["meaing"]}>
              定义图计算任务依赖的输入表、输出表结构。
            </span>
          </p>
          <div className={styles["definition-table"]}>
            <div className={styles["definition-header"]}>
              <div className={styles["title"]}>表定义列表</div>
              <div>
                <Search
                  style={{ width: 286, marginRight: 16 }}
                  placeholder="请输入搜索关键词"
                  onSearch={handleSearch}
                />
                <Button
                  type="primary"
                  onClick={() =>
                    setShowCreatePage({
                      visible: true,
                      currentItem: null,
                      realodedList: false,
                      readonly: false,
                    })
                  }
                >
                  新增
                </Button>
              </div>
            </div>
            <Table
              dataSource={state.list}
              columns={columns}
              pagination={{
                hideOnSinglePage: true,
                showQuickJumper: true,
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
