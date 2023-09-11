import React, { useEffect, useState } from "react";
import { Input, Row, Col, Form, Card, Button, message } from "antd";
import {
  createTableDefinition,
  updateTableDefinition,
  tableDetail,
} from "../services/tableDefinition";
import { GraphDefintionTab } from "../graph-tabs";
import { isEmpty } from "lodash";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

const CreateTableDefinition = ({ currentItem, toBackList, readonly }) => {
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const { value: instanceName } = currentInstance;
  const [form] = Form.useForm();
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [details, setDeatils] = useState<object>({});
  const [activeKey, setActiveKey] = useState<string>("TABLE");

  useEffect(() => {
    if (currentItem) {
      tableDetail(instanceName, currentItem.name).then((res) => {
        if (res.success) {
          setDeatils(res.data);
          form.setFieldsValue(res.data);
        }
      });
    }
  }, [currentItem]);

  const handleFileds = (value: any, name: string) => {
    return value?.map((item: any) => {
      return {
        name: item[name],
        type: item.type,
        comment: item.comment,
        category: item.category,
      };
    });
  };

  const onSave = async () => {
    const values = await form.getFieldsValue();
    const { tableConfig = {}, name, comment } = values;
    const fields = Object.values(values)?.filter(
      (str) => str && Object.keys(str).indexOf("tableName0") !== -1
    );

    setIsLoading(true);
    const { type, config = [] } = tableConfig;
    const configObj = {
      type,
      config: {},
    };
    for (const item of config) {
      configObj.config[item.key] = item.value;
    }

    if (!isEmpty(details)) {
      // 修改
      const updateParams = {
        ...details,
        name,
        comment,
        fields: handleFileds(fields, "tableName0"),
        pluginConfig: {
          ...details?.pluginConfig,
          type: type || details?.pluginConfig.type,
          config:
            config.length > 0 ? configObj.config : details?.pluginConfig.config,
        },
        type: "TABLE",
      };

      const result = await updateTableDefinition(
        instanceName,
        details?.name,
        updateParams
      );

      if (result.code !== "SUCCESS") {
        message.error(
          $i18n.get(
            {
              id: "openpiece-geaflow.geaflow.table-definition.create.FailedToUpdateTableDefinition",
              dm: "更新表定义失败：{resultMessage}",
            },
            { resultMessage: result.message }
          )
        );
      } else {
        message.success(
          $i18n.get({
            id: "openpiece-geaflow.geaflow.table-definition.create.TheTableDefinitionHasBeen",
            dm: "更新表定义成功",
          })
        );
        if (toBackList) {
          toBackList({
            visible: false,
            currentItem: null,
            realodedList: true,
            editable: false,
          });
        }
      }

      return;
    }

    if (tableConfig.type) {
      const createParams = {
        name,
        comment,
        fields: handleFileds(fields, "tableName0"),
        pluginConfig: configObj,
        type: "TABLE",
      };
      const result = await createTableDefinition(instanceName, createParams);
      setIsLoading(false);
      if (result.code !== "SUCCESS") {
        message.error(
          $i18n.get(
            {
              id: "openpiece-geaflow.geaflow.table-definition.create.FailedToCreateTableDefinition",
              dm: "创建表定义失败：{resultMessage}",
            },
            { resultMessage: result.message }
          )
        );
      } else {
        message.success(
          $i18n.get({
            id: "openpiece-geaflow.geaflow.table-definition.create.TheTableDefinitionIsCreated",
            dm: "创建表定义成功",
          })
        );
        if (toBackList) {
          toBackList({
            visible: false,
            currentItem: null,
            realodedList: true,
            editable: false,
          });
        }
      }
    } else {
      message.info(
        $i18n.get({
          id: "openpiece-geaflow.geaflow.table-definition.create.EnterParameterConfiguration",
          dm: "请选择参数配置",
        })
      );
      setActiveKey("tableConfig");
    }
    setIsLoading(false);
  };

  let defaultFormValues = {};

  // 根据 currentItem 来判断是新增还是修改
  if (!isEmpty(details)) {
    console.log(details);
    // 修改，设置表单初始值
    const { pluginConfig } = details;
    const configArr = [];
    for (const key in pluginConfig.config) {
      const current = pluginConfig.config[key];
      configArr.push({
        key,
        value: current,
      });
    }
    defaultFormValues = {
      tableConfig: {
        type: pluginConfig.type,
        config: configArr,
      },
      edges: details?.edges,
      vertices: details?.vertices,
    };
  } else {
    defaultFormValues = {
      tableConfig: {
        config: [],
      },
    };
  }

  const handleCancel = () => {
    if (toBackList) {
      toBackList({
        visible: false,
        currentItem: null,
        realodedList: false,
        editable: false,
      });
    }
  };
  return (
    <div className={styles["graph-definition"]}>
      <p className={styles["add-title"]}>
        {readonly
          ? $i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.create.TableDefinitionDetails",
              dm: "表定义详情",
            })
          : currentItem
          ? $i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.create.EditTableDefinitions",
              dm: "编辑表定义",
            })
          : $i18n.get({
              id: "openpiece-geaflow.geaflow.table-definition.create.AddTableDefinitions",
              dm: "新增表定义",
            })}
      </p>
      <Form form={form} layout="vertical" initialValues={defaultFormValues}>
        <Card
          title={$i18n.get({
            id: "openpiece-geaflow.geaflow.table-definition.create.BasicInformation",
            dm: "基本信息",
          })}
          className={styles["add-col"]}
          type="inner"
        >
          <Row gutter={24}>
            <Col span={12}>
              <Form.Item
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.table-definition.create.TableName",
                  dm: "表名称",
                })}
                name="name"
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.table-definition.create.EnterATableName",
                      dm: "请输入表名称",
                    }),
                  },
                ]}
              >
                <Input disabled={readonly} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.table-definition.create.Instance",
                  dm: "所属实例",
                })}
                name="instanceName"
              >
                <p>{instanceName}</p>
              </Form.Item>
            </Col>

            <Col span={24}>
              <Form.Item
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.table-definition.create.TableDescription",
                  dm: "表描述",
                })}
                name="comment"
              >
                <Input.TextArea rows={1} disabled={readonly} />
              </Form.Item>
            </Col>
          </Row>
        </Card>

        <GraphDefintionTab
          tabsList={[
            {
              name: $i18n.get({
                id: "openpiece-geaflow.geaflow.table-definition.create.TableDefinition",
                dm: "表定义",
              }),
              type: "TABLE",
              editTables: currentItem ? [] : [{}],
            },
            {
              name: $i18n.get({
                id: "openpiece-geaflow.geaflow.table-definition.create.ParameterConfiguration",
                dm: "参数配置",
              }),
              type: "tableConfig",
              editTables: [],
            },
          ]}
          form={form}
          currentItem={details}
          readonly={readonly}
          activeKey={activeKey}
          setActiveKey={setActiveKey}
        />

        {!readonly && (
          <div className={styles["definition-bottom"]}>
            <Button className={styles["bottom-btn"]} onClick={handleCancel}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.table-definition.create.Cancel",
                dm: "取消",
              })}
            </Button>
            <Button
              className={styles["bottom-btn"]}
              type="primary"
              htmlType="submit"
              onClick={onSave}
              loading={isLoading}
            >
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.table-definition.create.Submit",
                dm: "提交",
              })}
            </Button>
          </div>
        )}
      </Form>
    </div>
  );
};

export default CreateTableDefinition;
