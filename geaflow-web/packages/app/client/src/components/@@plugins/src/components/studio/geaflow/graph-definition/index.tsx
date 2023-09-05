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
  getGraphDefinitionList,
  deleteGraphDefinition,
} from "../services/graphDefinition";
import CreateGraphDefinition from "./create";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

const { Search } = Input;

export const GraphDefinition: React.FC<{}> = ({}) => {
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
    editable: false,
  });

  const handelTemplata = async () => {
    const manageData = await getGraphDefinitionList({
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-definition.GraphName",
        dm: "图名称",
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
                editable: false,
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
        id: "openpiece-geaflow.geaflow.graph-definition.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.graph-definition.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.graph-definition.ModifiedByRecordmodifiername",
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
        id: "openpiece-geaflow.geaflow.graph-definition.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.graph-definition.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.graph-definition.ModificationTimeRecordmodifytime",
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
        id: "openpiece-geaflow.geaflow.graph-definition.Operation",
        dm: "操作",
      }),

      render: (_, record) => (
        <Space>
          <a
            onClick={() =>
              setShowCreatePage({
                visible: true,
                currentItem: record.name,
                realodedList: false,
                readonly: false,
                editable: true,
              })
            }
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.Edit",
              dm: "编辑",
            })}
          </a>
          <Popconfirm
            title={$i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.AreYouSureYouWant",
              dm: "确认删除？",
            })}
            onConfirm={() => {
              deleteGraphDefinition(instanceName, record?.name).then((res) => {
                if (res?.success) {
                  message.success(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.graph-definition.DeletedSuccessfully",
                      dm: "删除成功",
                    })
                  );
                  handelTemplata();
                }
              });
            }}
            okText={$i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.Ok",
              dm: "确定",
            })}
            cancelText={$i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.Cancel",
              dm: "取消",
            })}
          >
            <a>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.graph-definition.Delete",
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
          <Breadcrumb style={{ marginBottom: 16 }}>
            <Breadcrumb.Item>
              <a
                onClick={() =>
                  setShowCreatePage({
                    visible: false,
                    currentItem: null,
                    realodedList: false,
                    readonly: false,
                    editable: false,
                  })
                }
              >
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.graph-definition.Graphs",
                  dm: "图定义",
                })}
              </a>
            </Breadcrumb.Item>
            <Breadcrumb.Item>
              {showCreatePage.editable
                ? $i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-definition.EditGraphDefinition",
                    dm: "编辑图定义",
                  })
                : showCreatePage.readonly
                ? $i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-definition.FigureDefinitionDetails",
                    dm: "图定义详情",
                  })
                : $i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-definition.AddAGraphDefinition",
                    dm: "新增图定义",
                  })}
            </Breadcrumb.Item>
          </Breadcrumb>
          <CreateGraphDefinition
            graphName={showCreatePage.currentItem}
            readonly={showCreatePage.readonly}
            editable={showCreatePage.editable}
            toBackList={setShowCreatePage}
          />
        </>
      ) : (
        <div className={styles["definition"]}>
          <p>
            <span className={styles["definition-title"]}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.graph-definition.GraphDefinition",
                dm: "图定义",
              })}
            </span>
            <span className={styles["meaing"]}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.graph-definition.DefinesTheGraphStructureOn",
                dm: "定义图计算任务依赖的图结构。",
              })}
            </span>
          </p>
          <div className={styles["definition-table"]}>
            <div className={styles["definition-header"]}>
              <div className={styles["title"]}>
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.graph-definition.GraphDefinitionList",
                  dm: "图定义列表",
                })}
              </div>
              <div>
                <Search
                  style={{ width: 286, marginRight: 16 }}
                  placeholder={$i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-definition.EnterASearchKeyword",
                    dm: "请输入搜索关键词",
                  })}
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
                      editable: false,
                    })
                  }
                >
                  {$i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-definition.Add",
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
                size: "small",
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
