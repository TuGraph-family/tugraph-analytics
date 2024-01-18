import { EditableProTable } from "@ant-design/pro-components";
import type { ProColumns } from "@ant-design/pro-components";
import { EditableFormInstance } from "@ant-design/pro-components";
import {
  Form,
  Input,
  Select,
  Collapse,
  Button,
  Row,
  Col,
  Table,
  Tag,
} from "antd";
import styles from "./graphDefintionEditTable.less";
import React, { useEffect, useState, useRef } from "react";
import { cloneDeep, isEmpty } from "lodash";
import $i18n from "@/components/i18n";

type DataSourceType = {
  id: React.Key;
  name?: string;
  comment?: string;
  type?: string;
  tag?: string;
};
type Props = {
  genExtra: any;
  paneIndex: any;
  form?: any;
  tabsData?: any;
  setTabsData?: any;
  tableList?: [];
  fieldMappings?: any;
  serveList: any;
  check: boolean;
  item?: object;
};
const { Panel } = Collapse;
export const GraphDefintionEditTable: React.FC<Props> = ({
  genExtra,
  paneIndex,
  form,
  tabsData,
  setTabsData,
  serveList,
  tableList,
  check,
  item,
}) => {
  const editableFormRef = useRef<EditableFormInstance>();
  const [dataSource, setDataSource] = useState<readonly DataSourceType[]>([]);
  const [editableKeys, setEditableRowKeys] = useState<React.Key[]>(() => {
    return dataSource?.map((item) => item.id);
  });
  const structNameWatch = Form.useWatch(`structName${paneIndex}`, form);
  const tableNameWatch = Form.useWatch(`tableName${paneIndex}`, form);

  const [tableFieldOptions, setTableFieldOptions] = useState([]);
  const [structFieldOptions, setStructFieldOptions] = useState([]);

  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const instanceName = currentInstance.value;

  useEffect(() => {
    const attributeName = [
      ...serveList[0].edges,
      ...serveList[0].vertices,
    ].filter((item) => item.name === structNameWatch);

    setStructFieldOptions(attributeName[0]?.fields);
  }, [structNameWatch]);

  useEffect(() => {
    const options = tableList.filter((item) => item.name === tableNameWatch);
    setTableFieldOptions(options[0]?.fields);
  }, [tableNameWatch]);

  const handleStructMapping = (value: any, paneIndex: number) => {
    return value?.map(
      (item: { tableFieldName: string; structFieldName: string }, idx) => {
        return {
          [`tableName${paneIndex}`]: item.tableFieldName,
          structFieldName: item.structFieldName,
          id: paneIndex + "-" + idx + "-" + Math.random().toFixed(6).slice(-6),
        };
      }
    );
  };

  useEffect(() => {
    const fieldMappings = handleStructMapping(item.fieldMappings, paneIndex);

    if (!isEmpty(fieldMappings)) {
      setDataSource(fieldMappings);
      setEditableRowKeys(() => {
        return fieldMappings?.map((item) => item.id);
      });
    }
    if (!isEmpty(item)) {
      form.setFieldsValue({
        [`tableName${paneIndex}`]: item.tableName,
        [`structName${paneIndex}`]: item.structName,
      });
    }
  }, [item]);

  const tagColorMap = {
    BIGINT: "green",
    VARCHAR: "blue",
    INT: "magenta",
    BOOLEAN: "yellow",
    TIMESTAMP: "orange",
    DOUBLE: "lightblue",
  };
  const handleValueEnum = (data) => {
    if (!isEmpty(data)) {
      const valueEnum = {};
      data.forEach(
        (item) =>
          (valueEnum[item.name] = {
            text: (
              <div>
                {item.name}&nbsp;&nbsp;&nbsp;
                <Tag
                  style={{
                    float: "right",
                    textAlign: "center",
                    marginRight: 10,
                    marginTop: 4,
                  }}
                  color={tagColorMap[`${item.type}`]}
                >
                  {item.type}
                </Tag>
              </div>
            ),
          })
      );
      return valueEnum;
    }
  };

  const checkColumns = [
    {
      title: `${$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldName",
        dm: "字段名",
      })}`,
      dataIndex: `tableName${paneIndex}`,
      key: `tableName${paneIndex}`,
    },
    {
      title: `${$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.PropertyName",
        dm: "属性名",
      })}`,
      dataIndex: "structFieldName",
      key: "structFieldName",
    },
  ];

  const columns: ProColumns<DataSourceType>[] = [
    {
      title: `${$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.FieldName",
        dm: "字段名",
      })}`,
      dataIndex: `tableName${paneIndex}`,
      valueType: "select",

      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: "请选择表字段名",
          },
        ],
      },
      valueEnum: handleValueEnum(tableFieldOptions),
      fieldProps: (_, { rowIndex }) => {
        return {
          disabled: check,
          placeholder: `${$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseSelect",
            dm: "请选择",
          })}`,
          notFoundContent: (
            <span style={{ color: "gray", height: 30 }}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseFirst",
                dm: "请先选择输入表",
              })}
            </span>
          ),
          onChange: (value) => {
            if (rowIndex < tabsData[paneIndex].fieldMappings.length) {
              tabsData[paneIndex].fieldMappings[rowIndex].tableFieldName =
                value;
            } else {
              tabsData[paneIndex].fieldMappings.push({ tableFieldName: value });
            }
            setTabsData(cloneDeep(tabsData));
          },
        };
      },
    },
    {
      title: `${$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.PropertyName",
        dm: "属性名",
      })}`,
      key: "structFieldName",
      dataIndex: "structFieldName",
      valueType: "select",
      formItemProps: {
        rules: [
          {
            required: true,
            whitespace: true,
            message: "请选择属性名",
          },
        ],
      },
      valueEnum: handleValueEnum(structFieldOptions),
      fieldProps: (_, { rowIndex }) => {
        return {
          disabled: check,
          placeholder: `${$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseSelect",
            dm: "请选择",
          })}`,
          notFoundContent: (
            <span style={{ color: "gray", height: 30 }}>请先选择目标点边</span>
          ),
          onChange: (value) => {
            if (rowIndex < tabsData[paneIndex].fieldMappings.length) {
              tabsData[paneIndex].fieldMappings[rowIndex].structFieldName =
                value;
            } else {
              tabsData[paneIndex].fieldMappings.push({ tableFieldName: value });
            }
            setTabsData(cloneDeep(tabsData));
          },
        };
      },
    },
    ...(!check
      ? [
          {
            title: $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Operation",
              dm: "操作",
            }),
            valueType: "option",
            width: 100,
          },
        ]
      : []),
  ];

  const table = (columnsType?: string) => (
    <EditableProTable<DataSourceType>
      columns={columns}
      editableFormRef={editableFormRef}
      rowKey="id"
      value={dataSource}
      onChange={(value) => {
        tabsData[paneIndex].fieldMappings = value.map((item, idx) => {
          return {
            tableFieldName: item[`tableName${paneIndex}`],
            structFieldName: item["structFieldName"],
          };
        });

        setTabsData(tabsData);
        setDataSource(value);
      }}
      maxLength={structFieldOptions && !check ? structFieldOptions.length : 0}
      recordCreatorProps={{
        newRecordType: "dataSource",
        creatorButtonText: `${$i18n.get({
          id: "openpiece-geaflow.geaflow.table-definition.AddRow",
          dm: "新增一行数据",
        })}`,
        record: () => ({
          id: Date.now(),
        }),
        disabled: check,
      }}
      editable={{
        deleteText: `${$i18n.get({
          id: "openpiece-geaflow.geaflow.table-definition.Delete",
          dm: "删除",
        })}`,
        deletePopconfirmMessage: `${$i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.add.AreYouSureYouWant",
          dm: "你确定要删除吗?",
        })}`,
        form: form,
        type: "multiple",
        editableKeys,
        actionRender: (row, config, defaultDoms) => {
          return check ? [] : [defaultDoms.delete];
        },
        onChange: setEditableRowKeys,
      }}
    />
  );

  const checkTable = () => (
    <Table columns={checkColumns} dataSource={dataSource} checkTable />
  );
  return (
    <Collapse ghost className={styles["collapse-bg"]} defaultActiveKey={["1"]}>
      <Panel
        key="1"
        extra={genExtra(paneIndex)}
        header={`${
          tableNameWatch ||
          `${$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.uploadModal.InputTable",
            dm: "输入表",
          })}`
        } -> ${
          structNameWatch ||
          `${$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.uploadModal.TargetStruct",
            dm: "目标点边",
          })}`
        }`}
      >
        <Row gutter={24}>
          <Col span={12}>
            <Form.Item
              name={`tableName${paneIndex}`}
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.InputTable",
                dm: "输入表",
              })}
              rules={[
                {
                  required: true,
                  message: `${$i18n.get({
                    id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseInputTable",
                    dm: "请选择输入表",
                  })}`,
                },
              ]}
            >
              <Select
                placeholder={$i18n.get({
                  id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseInputTable",
                  dm: "请选择输入表",
                })}
                disabled={check}
                onChange={(value, i) => {
                  if (tabsData[paneIndex].fieldMappings) {
                    const mappingLength =
                      tabsData[paneIndex].fieldMappings.length;
                    const n = Math.min(i.fields.length, mappingLength);
                    for (let idx = 0; idx < n; idx++) {
                      tabsData[paneIndex].fieldMappings[idx].tableFieldName =
                        i.fields[idx]["name"];
                      // dataSource[idx][`tableName${paneIndex}`]=i.fields[idx]['name'];
                    }

                    for (let idx = n; idx < mappingLength; idx++) {
                      if (
                        tabsData[paneIndex].fieldMappings[idx].structFieldName
                      ) {
                        tabsData[paneIndex].fieldMappings[idx].tableFieldName =
                          null;
                      }
                    }
                  } else {
                    const mappings = [];
                    for (let idx = 0; idx < i.fields.length; idx++) {
                      mappings.push({ tableFieldName: i.fields[idx]["name"] });
                    }
                    tabsData[paneIndex].fieldMappings = mappings;
                  }

                  setTableFieldOptions(i.fields);
                  tabsData[paneIndex]["tableName"] = value;
                  setTabsData(cloneDeep(tabsData));
                }}
              >
                {tableList?.map((item: { name: string; id: string }) => {
                  return (
                    <Select.Option
                      key={item.id}
                      value={item.name}
                      fields={item.fields}
                    >
                      {item.name}
                    </Select.Option>
                  );
                })}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              name={`structName${paneIndex}`}
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.TargetStruct",
                dm: "目标点边",
              })}
              rules={[
                {
                  required: true,
                  message: `${$i18n.get({
                    id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseTarget",
                    dm: "请选择输入表",
                  })}`,
                },
              ]}
              //  initialValue={structNames}
            >
              <Select
                placeholder={$i18n.get({
                  id: "openpiece-geaflow.geaflow.function-manage.uploadModal.PleaseTarget",
                  dm: "请选择输入表",
                })}
                disabled={check}
                onChange={(value, i) => {
                  setStructFieldOptions(i.fields);

                  if (tabsData[paneIndex].fieldMappings) {
                    const n = Math.min(
                      i.fields.length,
                      tabsData[paneIndex].fieldMappings.length
                    );
                    for (let idx = 0; idx < n; idx++) {
                      tabsData[paneIndex].fieldMappings[idx].structFieldName =
                        i.fields[idx]["name"];
                      ///  fieldMappings[idx][`tableName${paneIndex}`]=i.fields[idx]['name'];
                      //  dataSource[idx][`structName${paneIndex}`] = i.fields[idx]['name'];
                    }

                    for (let idx = n; idx < i.fields.length; idx++) {
                      tabsData[paneIndex].fieldMappings.push({
                        structFieldName: i.fields[idx]["name"],
                      });
                      ///  fieldMappings[idx][`tableName${paneIndex}`]=i.fields[idx]['name'];
                      //  dataSource[idx][`structName${paneIndex}`] = i.fields[idx]['name'];
                    }
                  } else {
                    const mappings = [];
                    for (let idx = 0; idx < i.fields.length; idx++) {
                      mappings.push({ structFieldName: i.fields[idx]["name"] });
                    }
                    tabsData[paneIndex].fieldMappings = mappings;
                  }
                  tabsData[paneIndex]["structName"] = value;
                  setTabsData(cloneDeep(tabsData));
                }}
              >
                <Select.OptGroup
                  label={$i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Vertex",
                    dm: "点",
                  })}
                >
                  {serveList[0]?.vertices?.map((item) => {
                    return (
                      <Select.Option
                        value={item?.name}
                        key={item?.id}
                        label={item.name}
                        fields={item.fields}
                      >
                        <div>
                          {item?.name}&nbsp;&nbsp;&nbsp;
                          <Tag
                            style={{
                              float: "right",
                              textAlign: "center",
                              marginRight: 10,
                              marginTop: 4,
                            }}
                            color={"green"}
                          >
                            VERTEX
                          </Tag>
                        </div>
                      </Select.Option>
                    );
                  })}
                </Select.OptGroup>
                <Select.OptGroup
                  label={$i18n.get({
                    id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.Edge",
                    dm: "边",
                  })}
                >
                  {serveList[0]?.edges?.map((item) => {
                    return (
                      <Select.Option
                        value={item?.name}
                        key={item?.id}
                        label={item.name}
                        fields={item.fields}
                      >
                        <div>
                          {item?.name}&nbsp;&nbsp;&nbsp;
                          <Tag
                            style={{
                              float: "right",
                              textAlign: "center",
                              marginRight: 10,
                              marginTop: 4,
                            }}
                            color={"blue"}
                          >
                            EDGE
                          </Tag>
                        </div>
                      </Select.Option>
                    );
                  })}
                </Select.OptGroup>
              </Select>
            </Form.Item>
          </Col>
        </Row>
        {check ? checkTable() : table()}
      </Panel>
    </Collapse>
  );
};
