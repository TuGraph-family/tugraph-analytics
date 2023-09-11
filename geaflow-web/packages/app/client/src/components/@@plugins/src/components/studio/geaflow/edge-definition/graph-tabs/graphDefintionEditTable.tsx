import { EditableProTable } from "@ant-design/pro-components";
import type { ProColumns } from "@ant-design/pro-components";
import { EditableFormInstance } from "@ant-design/pro-components";
import { Form, Input, Collapse, Row, Col, Table, message } from "antd";
import React, { useState, useRef } from "react";
import { useTranslation } from "react-i18next";
import styles from "./graphDefintionEditTable.less";
import $i18n from "@/components/i18n";

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
  index,
  paneIndex,
  form,
  fields,
  name,
  readonly,
  editable,
}) => {
  const defaultData = [
    {
      id: (Date.now() + 0).toString(),
      name: "src_id",
      type: "BIGINT",
      category: "EDGE_SOURCE_ID",
      comment: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SourcePointId",
        dm: "源点ID",
      }),
    },
    {
      id: (Date.now() + 1).toString(),
      name: "dst_id",
      type: "BIGINT",
      category: "EDGE_TARGET_ID",
      comment: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.TargetPointId",
        dm: "目标点ID",
      }),
    },
    {
      id: (Date.now() + 2).toString(),
      name: "ts",
      type: "BIGINT",
      category: "EDGE_TIMESTAMP",
      comment: $i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EdgeTimestamp",
        dm: "边时间戳",
      }),
    },
  ];
  const editableFormRef = useRef<EditableFormInstance>();
  const [dataSource, setDataSource] = useState<readonly DataSourceType[]>(
    fields || defaultData
  );
  const categoryType = {
    EDGE: {
      EDGE_SOURCE_ID: {
        text: $i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.SourcePointId",
          dm: "源点ID",
        }),
      },
      EDGE_TARGET_ID: {
        text: $i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.TargetPointId",
          dm: "目标点ID",
        }),
      },
      EDGE_TIMESTAMP: {
        text: $i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EdgeTimestamp",
          dm: "边时间戳",
        }),
      },
      PROPERTY: {
        text: $i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.EdgeProperties",
          dm: "边属性",
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
      dataIndex: "name",
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
      dataIndex: "name",
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

  return <>{table()}</>;
};
