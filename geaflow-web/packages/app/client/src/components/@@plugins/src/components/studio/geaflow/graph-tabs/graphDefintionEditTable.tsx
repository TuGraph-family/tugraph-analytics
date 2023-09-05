import { EditableProTable } from "@ant-design/pro-components";
import type { ProColumns } from "@ant-design/pro-components";
import { EditableFormInstance } from "@ant-design/pro-components";
import { Form, Input, Collapse, Row, Col, Table, message } from "antd";
import React, { useState, useRef } from "react";
import { useTranslation } from "react-i18next";
import styles from "./graphDefintionEditTable.less";
import $i18n from "../../../../../../i18n";

type DataSourceType = {
  id: React.Key;
  name?: string;
  desc?: string;
  type?: string;
  tag?: string;
};

type Props = {
  // type: "VERTEX" | "EDGE" | "paramConfig";
  type: string;
  length?: number;
  genExtra: any;
  id?: number;
  paneIndex: number;
  index: number;
  form?: any;
  key?: any;
  fields?: any;
  name?: string;
  readonly?: boolean;
  editable?: boolean;
};
const { Panel } = Collapse;

export const GraphDefintionEditTable: React.FC<Props> = ({
  type,
  genExtra,
  index,
  paneIndex,
  form,
  fields,
  name,
  readonly,
  editable,
}) => {
  const pointName = Form.useWatch(`pointName${paneIndex}`, form);
  const sideName = Form.useWatch(`sideName${paneIndex}`, form);
  const inputName = Form.useWatch(`inputName${paneIndex}`, form);
  const editableFormRef = useRef<EditableFormInstance>();
  const [dataSource, setDataSource] =
    useState<readonly DataSourceType[]>(fields);

  const { t } = useTranslation();
  const [editDataSource, setEditDataSource] = useState<
    readonly DataSourceType[]
  >([]);
  const allName = {
    VERTEX: pointName,
    EDGE: sideName,
    TABLE: inputName,
  };
  const allType = {
    VERTEX: `pointName${paneIndex}`,
    EDGE: `sideName${paneIndex}`,
    TABLE: `tableName${paneIndex}`,
  };

  const categoryType = {
    TABLE: {
      ID: {
        text: "ID",
      },
      PROPERTY: {
        text: $i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Properties",
          dm: "属性",
        }),
      },
    },
  };

  const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => {
    return dataSource?.map((item) => item.id);
  });

  const columns: ProColumns<DataSourceType>[] = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldName",
        dm: "字段名",
      }),
      dataIndex: allType[type],
      width: "30%",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EnterAFieldName",
              dm: "请输入字段名",
            }),
          },
        ],
      },
      fieldProps: (_, { rowIndex }) => {
        if (readonly || (editable && !!name)) {
          return {
            disabled: true,
          };
        }
      },
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldType",
        dm: "字段类型",
      }),
      key: "type",
      dataIndex: "type",
      valueType: "select",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SelectAFieldType",
              dm: "请选择字段类型",
            }),
          },
        ],
      },
      valueEnum: {
        VARCHAR: {
          text: "VARCHAR",
        },
        INT: {
          text: "INT",
        },
        BOOlEAN: {
          text: "BOOlEAN",
        },
        BIGINT: {
          text: "BIGINT",
        },
        DOUBLE: {
          text: "DOUBLE",
        },
        TIMESTAMP: {
          text: "TIMESTAMP",
        },
      },
      fieldProps: (_, { rowIndex }) => {
        if (dataSource[rowIndex]?.tag === "VERTEX_TYPE") {
          // editableFormRef.current?.setRowData?.(rowIndex, { type: 'STRING' });
          return {
            disabled: true,
          };
        }
        // if (type === "EDGE" && [2, 3].includes(rowIndex)) {
        //   return {
        //     disabled: true,
        //   };
        // }
        if (readonly || (editable && !!name)) {
          return {
            disabled: true,
          };
        }
      },
    },

    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldConstraints",
        dm: "字段约束",
      }),
      dataIndex: "category",
      valueType: "select",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SelectAFieldType",
              dm: "请选择字段类型",
            }),
          },
        ],
      },
      valueEnum: categoryType[type],
      fieldProps: (_, { rowIndex }) => {
        if (readonly || (editable && !!name)) {
          return {
            disabled: true,
          };
        }
        return {
          onSelect: (value: string) => {
            if (type === "VERTEX") {
              if (value === "VERTEX_ID") {
                if (dataSource?.some((i) => i.category === value)) {
                  message.error(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.PointEdgeVAlreadyContains",
                      dm: "点/边v已包含字段约束",
                    })
                  );
                  editableFormRef.current?.setRowData?.(rowIndex, {
                    category: [],
                  });
                }
              }
            }
            if (type === "EDGE") {
              if (
                ["EDGE_SOURCE_ID", "EDGE_TARGET_ID", "EDGE_TIMESTAMP"].includes(
                  value
                )
              ) {
                if (dataSource?.some((i) => i.category === value)) {
                  message.error(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.PointEdgeVAlreadyContains",
                      dm: "点/边v已包含字段约束",
                    })
                  );
                  editableFormRef.current?.setRowData?.(rowIndex, {
                    category: [],
                  });
                }
              }
            }
          },
        };
      },
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Remarks",
        dm: "备注",
      }),
      dataIndex: "comment",
      fieldProps: (_, { rowIndex }) => {
        if (readonly || (editable && !!name)) {
          return {
            disabled: true,
          };
        }
      },
    },

    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Operation",
        dm: "操作",
      }),
      valueType: readonly || (editable && !!name) ? "" : "option",
      width: 100,
    },
  ];

  const showColumns = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldName",
        dm: "字段名",
      }),
      dataIndex: allType[type],
      width: "30%",
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldType",
        dm: "字段类型",
      }),
      key: "type",
      dataIndex: "type",
    },

    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldConstraints",
        dm: "字段约束",
      }),
      dataIndex: "category",
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Remarks",
        dm: "备注",
      }),
      dataIndex: "comment",
    },
  ];

  const table = () => {
    if (readonly && !editable) {
      const tableData = fields.map((d) => {
        return {
          ...d,
          category: categoryType[type][d.category].text,
        };
      });
      return (
        <Table
          columns={showColumns}
          dataSource={tableData}
          pagination={{ hideOnSinglePage: true }}
        />
      );
    }

    return (
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
          disabled: readonly || (editable && !!name),
        }}
        editable={{
          form: form,
          type: "multiple",
          editableKeys,

          actionRender: (row, config, defaultDoms) => {
            return [defaultDoms.delete];
          },
          onValuesChange: (record, recordList) => {
            setDataSource(recordList);
          },
          onChange: setEditableRowKeys,
        }}
      />
    );
  };

  return type !== "TABLE" ? (
    <Collapse ghost className={styles["collapse-bg"]} defaultActiveKey={["1"]}>
      <Panel
        header={`${t("i18n.key.name")}：${allName[type] || ""}`}
        key="1"
        extra={
          readonly || (editable && !!name) ? null : genExtra(index, paneIndex)
        }
      >
        {type === "VERTEX" && (
          <Row gutter={24}>
            <Col span={12}>
              <Form.Item
                name={`pointName${paneIndex}`}
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.PointName",
                  dm: "点名称",
                })}
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EnterAPointName",
                      dm: "请输入点名称",
                    }),
                  },
                  () => ({
                    validator(rule, value) {
                      const values = form.getFieldsValue();
                      let vertices = [];
                      Object.keys(values)?.map((item) => {
                        if (
                          item.indexOf("pointName") !== -1 &&
                          item !== `pointName${paneIndex}`
                        ) {
                          vertices.push(values[item]);
                        }
                      });
                      if (vertices.includes(value)) {
                        return Promise.reject(
                          $i18n.get({
                            id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.TheDotNameAlreadyExists",
                            dm: "该点名称已存在!",
                          })
                        );
                      }
                      return Promise.resolve();
                    },
                  }),
                ]}
                initialValue={name}
              >
                <Input disabled={readonly || (editable && !!name)} />
              </Form.Item>
            </Col>
          </Row>
        )}

        {type === "EDGE" && (
          <Row gutter={24}>
            <Col span={12}>
              <Form.Item
                name={`sideName${paneIndex}`}
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EdgeName",
                  dm: "边名称",
                })}
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EnterAPointName",
                      dm: "请输入点名称",
                    }),
                  },
                  () => ({
                    validator(rule, value) {
                      const values = form.getFieldsValue();
                      let edges = [];
                      Object.keys(values)?.map((item) => {
                        if (
                          item.indexOf("sideName") !== -1 &&
                          item !== `sideName${paneIndex}`
                        ) {
                          edges.push(values[item]);
                        }
                      });
                      if (edges.includes(value)) {
                        return Promise.reject(
                          $i18n.get({
                            id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.TheEdgeNameAlreadyExists",
                            dm: "该边名称已存在!",
                          })
                        );
                      }
                      return Promise.resolve();
                    },
                  }),
                ]}
                initialValue={name}
              >
                <Input disabled={readonly || (editable && !!name)} />
              </Form.Item>
            </Col>
          </Row>
        )}
        {type === "VERTEX" &&
          $i18n.get({
            id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.VertexFields",
            dm: "点配置",
          })}
        {type === "EDGE" &&
          $i18n.get({
            id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EdgeFields",
            dm: "边配置",
          })}

        {table()}
      </Panel>
    </Collapse>
  ) : (
    <>{table()}</>
  );
};
