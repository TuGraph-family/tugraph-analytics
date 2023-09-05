import { Button, Tabs, Table, Collapse } from "antd";
import React, { useState, useEffect, useRef } from "react";
import type { ColumnsType } from "antd/es/table";
import { GraphDefinitionConfigPanel } from "./graphDefinitionConfigPanel";
import { isEmpty, cloneDeep } from "lodash";
import styles from "./index.less";
import $i18n from "@/components/i18n";
import { getEdgeDefinitionList } from "../../services/edgeDefinition";
import { getNodeDefinitionList } from "../../services/nodeDefinition";
import { EditableProTable } from "@ant-design/pro-components";
import { EditableFormInstance } from "@ant-design/pro-components";
import type { ProColumns } from "@ant-design/pro-components";
interface DataType {
  key: React.Key;
  name: string;
}
const { Panel } = Collapse;

type DataSourceType = {
  edgeName?: string;
  targetName?: string;
  sourceName?: string;
};

type Props = {
  tabsList: {
    name: string;
    type: string;
    editTables: any;
  }[];
  form: any;
  currentItem?: any;
  readonly: boolean;
  editable?: boolean;
  topology: any;
  setTopology: any;
  activeKey?: string;
  setActiveKey?: any;
};

const { TabPane } = Tabs;
export const GraphDefintionTab: React.FC<Props> = ({
  tabsList = [],
  form,
  currentItem,
  readonly,
  editable,
  topology,
  setTopology,
  activeKey,
  setActiveKey,
}) => {
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const { value: instanceName } = currentInstance;
  const [tabsData, setTabsData] = useState(tabsList);
  const [state, setState] = useState({
    verticeData: [],
    edgeData: [],
    search: "",
    edgeText: {},
    vertexText: {},
  });
  const editableFormRef = useRef<EditableFormInstance>();
  const [dataSource, setDataSource] = useState<readonly DataSourceType[]>([]);
  const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => {
    return dataSource?.map((item) => item.id);
  });
  const handelTemplata = async () => {
    let verticeData;
    let edgeData;
    let edgeText = {};
    let vertexText = {};
    if (readonly) {
      verticeData = currentItem.vertices;
      edgeData = currentItem.edges;
    } else {
      verticeData = await getNodeDefinitionList(instanceName);
      edgeData = await getEdgeDefinitionList(instanceName);
    }

    if (!isEmpty(currentItem?.edges)) {
      currentItem?.edges.forEach((d) => {
        edgeText[d.name] = { text: d.name };
      });
    }
    if (!isEmpty(currentItem?.vertices)) {
      currentItem?.vertices.forEach((d) => {
        vertexText[d.name] = { text: d.name };
      });
    }
    setState({
      ...state,
      verticeData,
      edgeData,
      edgeText,
      vertexText,
    });
    if (!isEmpty(currentItem?.endpoints)) {
      const endpoints = currentItem.endpoints.map((item) => {
        return {
          id: item.edgeName + "-" + item.sourceName + "-" + item.targetName,
          edgeName: item.edgeName,
          sourceName: item.sourceName,
          targetName: item.targetName,
        };
      });
      setDataSource(endpoints);
      setEditableRowKeys(() => {
        return endpoints?.map((item) => item.id);
      });
    }
  };

  useEffect(() => {
    // 只有当实例存在时才查询
    if (instanceName) {
      handelTemplata();
    }
  }, [instanceName, currentItem]);

  const verticeColumns: ColumnsType<DataType> = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.table-definition.VerticsList",
        dm: "点列表",
      }),
      dataIndex: "name",
      key: "name",
    },
  ];
  const edgeColumns: ColumnsType<DataType> = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.table-definition.EdgeList",
        dm: "边列表",
      }),
      dataIndex: "name",
      key: "name",
    },
  ];

  // rowSelection object indicates the need for row selection
  const verticeSelection = {
    onChange: (selectedRowKeys: React.Key[], selectedRows: DataType[]) => {
      let vertexText = {};
      selectedRows.forEach((d) => {
        vertexText[d.name] = { text: d.name };
      });
      setState({ ...state, vertexText });
      setTopology({ ...topology, vertices: selectedRowKeys });
    },
    getCheckboxProps: (record: DataType) => ({
      disabled: readonly,
    }),
  };

  const edgeSelection = {
    onChange: (selectedRowKeys: React.Key[], selectedRows: DataType[]) => {
      selectedRowKeys.map((item) => {
        return {
          id: item,
        };
      });
      let edgeText = {};
      selectedRows.forEach((d) => {
        edgeText[d.name] = { text: d.name };
      });
      setState({ ...state, edgeText });
      setTopology({ ...topology, edges: selectedRowKeys });
    },
    getCheckboxProps: (record: DataType) => ({
      disabled: readonly,
    }),
  };

  const columns: ProColumns<DataSourceType>[] = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Edge",
        dm: "边",
      }),
      key: "edgeName",
      dataIndex: "edgeName",
      valueType: "select",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SelectAFieldEdge",
              dm: "请选择边",
            }),
          },
        ],
      },
      valueEnum: state.edgeText,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SourceVertex",
        dm: "源点",
      }),
      key: "sourceName",
      dataIndex: "sourceName",
      valueType: "select",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SelectAFieldSourceVertex",
              dm: "请选择源点",
            }),
          },
        ],
      },
      valueEnum: state.vertexText,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.TargetVertex",
        dm: "目标点",
      }),
      key: "targetName",
      dataIndex: "targetName",
      valueType: "select",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SelectAFieldTargetVertex",
              dm: "请选择目标点",
            }),
          },
        ],
      },
      valueEnum: state.vertexText,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Operation",
        dm: "操作",
      }),
      valueType: "option",
      width: 100,
    },
  ];

  return (
    <Tabs
      className={styles["graph-tab"]}
      activeKey={activeKey}
      onChange={(key) => {
        setActiveKey(key);
      }}
    >
      {tabsData.map((item, index) => (
        <TabPane tab={item.name} key={item.type}>
          {["tableConfig", "paramConfig"].includes(item.type) ? (
            <GraphDefinitionConfigPanel
              prefixName={
                item.type === "paramConfig" ? "pluginConfig" : "tableConfig"
              }
              form={form}
              readonly={!editable && readonly}
            />
          ) : (
            <div>
              <p>
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.table-definition.TopologyDefinition",
                  dm: "拓扑定义",
                })}
              </p>
              <div className={styles["graph-table"]}>
                <Table
                  columns={verticeColumns}
                  dataSource={state.verticeData}
                  rowSelection={{
                    type: "checkbox",
                    ...verticeSelection,
                    selectedRowKeys: topology.vertices,
                  }}
                  pagination={{
                    pageSize: 10,
                    hideOnSinglePage: true,
                  }}
                  bordered={true}
                  style={{ width: "47%" }}
                  rowKey={(record) => record.id}
                />
                <Table
                  columns={edgeColumns}
                  dataSource={state.edgeData}
                  rowSelection={{
                    type: "checkbox",
                    ...edgeSelection,
                    selectedRowKeys: topology.edges,
                  }}
                  pagination={{
                    pageSize: 10,
                    hideOnSinglePage: true,
                  }}
                  bordered
                  style={{ width: "47%" }}
                  rowKey={(record) => record.id}
                />
              </div>
              <Collapse ghost defaultActiveKey={["1"]}>
                <Panel
                  header={$i18n.get({
                    id: "openpiece-geaflow.geaflow.table-definition.Endpoint",
                    dm: "拓扑约束",
                  })}
                  key="1"
                >
                  {readonly ? (
                    <Table
                      columns={columns}
                      dataSource={currentItem?.endpoints || []}
                      pagination={{
                        pageSize: 10,
                        hideOnSinglePage: true,
                      }}
                    />
                  ) : (
                    <EditableProTable<DataSourceType>
                      columns={columns}
                      editableFormRef={editableFormRef}
                      rowKey="id"
                      value={dataSource}
                      onChange={setDataSource}
                      recordCreatorProps={{
                        newRecordType: "dataSource",
                        record: () => ({
                          id: Date.now(),
                        }),
                      }}
                      editable={{
                        form: form,
                        type: "multiple",
                        editableKeys,
                        actionRender: (row, config, defaultDoms) => {
                          return [defaultDoms.delete];
                        },
                        // onValuesChange: (record, recordList) => {
                        //   setDataSource(recordList);
                        // },
                        onChange: setEditableRowKeys,
                      }}
                    />
                  )}
                </Panel>
              </Collapse>
            </div>
          )}
        </TabPane>
      ))}
    </Tabs>
  );
};
