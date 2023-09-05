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
  getEdgeDefinitionList,
  deleteEdgeDefinition,
} from "../services/edgeDefinition";
import CreateTableDefinition from "./create";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

const { Search } = Input;

export const EdgeDefinition: React.FC<{}> = ({}) => {
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
    realodedList: false,
    readonly: false,
    currentItem: "",
  });

  const handelTemplata = async () => {
    const manageData = await getEdgeDefinitionList(instanceName, {
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.table-definition.EdgeName",
        dm: "边名称",
      }),
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <a
            onClick={() =>
              setShowCreatePage({
                visible: true,
                currentItem: record.name,
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.table-definition.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.table-definition.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.table-definition.ModifiedByRecordmodifiername",
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
        id: "openpiece-geaflow.geaflow.table-definition.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.table-definition.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.table-definition.ModificationTimeRecordmodifytime",
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
        id: "openpiece-geaflow.geaflow.table-definition.Operation",
        dm: "操作",
      }),
      render: (_, record) => (
        <Space>
          <a
            onClick={() =>
              setShowCreatePage({
                visible: true,
                realodedList: false,
                readonly: false,
                currentItem: record.name,
              })
            }
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.Edit",
              dm: "编辑",
            })}
          </a>
          <Popconfirm
            title={$i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.AreYouSureYouWant",
              dm: "确认删除？",
            })}
            onConfirm={() => {
              deleteEdgeDefinition(instanceName, record?.name).then((res) => {
                if (res?.success) {
                  message.success(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.table-definition.DeletedSuccessfully",
                      dm: "删除成功",
                    })
                  );
                  handelTemplata();
                }
              });
            }}
            okText={$i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.Ok",
              dm: "确定",
            })}
            cancelText={$i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.Cancel",
              dm: "取消",
            })}
          >
            <a>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.table-definition.Delete",
                dm: "删除",
              })}
            </a>
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
                    realodedList: false,
                    readonly: false,
                    currentItem: "",
                  })
                }
              >
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.table-definition.Edges",
                  dm: "边定义",
                })}
              </a>
            </Breadcrumb.Item>

            <Breadcrumb.Item>
              {showCreatePage.readonly
                ? $i18n.get({
                    id: "openpiece-geaflow.geaflow.table-definition.EdgeDefinitionDetails",
                    dm: "边定义详情",
                  })
                : showCreatePage.currentItem
                ? $i18n.get({
                    id: "openpiece-geaflow.geaflow.table-definition.EditEdgeDefinitions",
                    dm: "编辑边定义",
                  })
                : $i18n.get({
                    id: "openpiece-geaflow.geaflow.table-definition.AddEdgeDefinitions",
                    dm: "新增边定义",
                  })}
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
            <span className={styles["definition-title"]}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.table-definition.EdgesDefinition",
                dm: "边定义",
              })}
            </span>
            <span className={styles["meaing"]}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.edge-definition.DefineTheStructureOfThe",
                dm: "定义图依赖的边表结构。",
              })}
            </span>
          </p>
          <div className={styles["definition-table"]}>
            <div className={styles["definition-header"]}>
              <div className={styles["title"]}>
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.table-definition.EdgesDefinitionList",
                  dm: "边定义列表",
                })}
              </div>
              <div>
                <Search
                  style={{ width: 286, marginRight: 16 }}
                  placeholder={$i18n.get({
                    id: "openpiece-geaflow.geaflow.table-definition.EnterASearchKeyword",
                    dm: "请输入搜索关键词",
                  })}
                  onSearch={handleSearch}
                />

                <Button
                  type="primary"
                  onClick={() =>
                    setShowCreatePage({
                      visible: true,
                      realodedList: false,
                      readonly: false,
                      currentItem: "",
                    })
                  }
                >
                  {$i18n.get({
                    id: "openpiece-geaflow.geaflow.table-definition.Add",
                    dm: "新增",
                  })}
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
