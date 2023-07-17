import React, { useState } from "react";
import { Input, Row, Col, Form, Card, Button, message } from "antd";
import {
  createGraphDefinition,
  updateGraphDefinition,
} from "../services/graphDefinition";
import { GraphDefintionTab } from "../graph-tabs";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

const GraphDefinition = ({ currentItem, toBackList, readonly, editable }) => {
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const { value: instanceName } = currentInstance;

  const [form] = Form.useForm();
  const [isLoading, setIsLoading] = useState<boolean>(false);

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
    const values = await form.validateFields();
    setIsLoading(true);
    // 参数配置的值
    const { pluginConfig = {}, name, comment } = values;
    let vertices: { name: string; type: string; fields: any; id?: string }[] =
      [];
    const filterVertices = Object.keys(values).filter(
      (item) => !item.indexOf("pointName")
    );
    filterVertices?.map((item, index) => {
      const field = Object.values(values)?.filter(
        (str) => str && Object.keys(str).indexOf(item) !== -1
      );
      vertices.push({
        name: values[item],
        type: "VERTEX",
        fields: handleFileds(field, item),
        ...(editable &&
          currentItem.vertices[index]?.id !== 0 && {
            id: currentItem.vertices[index]?.id,
          }),
      });
    });
    let edges: { name: string; type: string; fields: any }[] = [];
    const filterEdges = Object.keys(values).filter(
      (item) => !item.indexOf("sideName")
    );
    filterEdges?.map((item, index) => {
      const field = Object.values(values)?.filter(
        (str) => str && Object.keys(str).indexOf(item) !== -1
      );
      edges.push({
        name: values[item],
        type: "EDGE",
        fields: handleFileds(field, item),
        ...(editable &&
          currentItem.edges[index]?.id !== 0 && {
            id: currentItem.edges[index]?.id,
          }),
      });
    });
    const { type, config = [] } = pluginConfig;
    const configObj = {
      type,
      config: {},
    };
    for (const item of config) {
      configObj.config[item.key] = item.value;
    }

    if (editable) {
      // 将用户新增的节点和原来的节点进行合并
      // 已有节点的 name 合集
      const updateParams = {
        ...currentItem,
        edges: edges || currentItem.edges,
        vertices,
        pluginConfig: {
          ...currentItem.pluginConfig,
          type: type || currentItem.pluginConfig.type,
          config:
            config.length > 0
              ? configObj.config
              : currentItem.pluginConfig.config,
        },
      };
      const updateResult = await updateGraphDefinition(
        instanceName,
        currentItem.name,
        updateParams
      );

      setIsLoading(false);

      if (updateResult.code !== "SUCCESS") {
        message.error(
          $i18n.get(
            {
              id: "openpiece-geaflow.geaflow.graph-definition.create.FailedToUpdateTheGraph",
              dm: "更新图定义失败：{updateResultMessage}",
            },
            { updateResultMessage: updateResult.message }
          )
        );
      } else {
        message.success(
          $i18n.get({
            id: "openpiece-geaflow.geaflow.graph-definition.create.TheGraphDefinitionHasBeen",
            dm: "更新图定义成功",
          })
        );
        if (toBackList) {
          toBackList({
            visible: false,
            currentItem: null,
            realodedList: true,
          });
        }
      }
      return;
    }

    const createParams = {
      name,
      vertices,
      edges,
      comment,
      pluginConfig: configObj,
    };

    const result = await createGraphDefinition(instanceName, createParams);

    setIsLoading(false);

    if (result.code !== "SUCCESS") {
      message.error(
        $i18n.get(
          {
            id: "openpiece-geaflow.geaflow.graph-definition.create.FailedToCreateAGraph",
            dm: "创建图定义失败：{resultMessage}",
          },
          { resultMessage: result.message }
        )
      );
    } else {
      message.success(
        $i18n.get({
          id: "openpiece-geaflow.geaflow.graph-definition.create.TheGraphIsDefined",
          dm: "创建图定义成功",
        })
      );
      if (toBackList) {
        toBackList({
          visible: false,
          currentItem: null,
          realodedList: true,
        });
      }
    }
  };

  let defaultFormValues = {};

  // 根据 currentItem 来判断是新增还是修改
  if (currentItem) {
    // 修改，设置表单初始值
    const { pluginConfig } = currentItem;
    const configArr = [];
    for (const key in pluginConfig.config) {
      const current = pluginConfig.config[key];
      configArr.push({
        key,
        value: current,
      });
    }
    defaultFormValues = {
      pluginConfig: {
        type: pluginConfig.type,
        config: configArr,
      },
      edges: currentItem.edges,
      vertices: currentItem.vertices,
      name: currentItem.name,
      comment: currentItem.comment,
    };
  } else {
    defaultFormValues = {
      pluginConfig: {
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
      });
    }
  };
  return (
    <div className={styles["graph-definition"]}>
      <p className={styles["add-title"]}>
        {editable
          ? $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.create.EditGraphDefinition",
              dm: "编辑图定义",
            })
          : readonly
          ? $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.create.FigureDefinitionDetails",
              dm: "图定义详情",
            })
          : $i18n.get({
              id: "openpiece-geaflow.geaflow.graph-definition.create.AddAGraphDefinition",
              dm: "新增图定义",
            })}
      </p>
      <Form form={form} layout="vertical" initialValues={defaultFormValues}>
        <Card
          title={$i18n.get({
            id: "openpiece-geaflow.geaflow.graph-definition.create.BasicInformation",
            dm: "基本信息",
          })}
          className={styles["add-col"]}
          type="inner"
        >
          <Row gutter={24}>
            <Col span={12}>
              <Form.Item
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.graph-definition.create.GraphName",
                  dm: "图名称",
                })}
                name="name"
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.graph-definition.create.EnterAMapName",
                      dm: "请输入图名称",
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
                  id: "openpiece-geaflow.geaflow.graph-definition.create.Instance",
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
                  id: "openpiece-geaflow.geaflow.graph-definition.create.FigureDescription",
                  dm: "图描述",
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
                id: "openpiece-geaflow.geaflow.graph-definition.create.PointDefinition",
                dm: "点定义",
              }),
              type: "VERTEX",
              editTables: [],
            },
            {
              name: $i18n.get({
                id: "openpiece-geaflow.geaflow.graph-definition.create.EdgeDefinition",
                dm: "边定义",
              }),
              type: "EDGE",
              editTables: [],
            },
            {
              name: $i18n.get({
                id: "openpiece-geaflow.geaflow.graph-definition.create.ParameterConfiguration",
                dm: "参数配置",
              }),
              type: "paramConfig",
              editTables: [],
            },
          ]}
          form={form}
          currentItem={currentItem}
          readonly={readonly}
          editable={editable}
        />

        {(!readonly || editable) && (
          <div className={styles["definition-bottom"]}>
            <Button className={styles["bottom-btn"]} onClick={handleCancel}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.graph-definition.create.Cancel",
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
                id: "openpiece-geaflow.geaflow.graph-definition.create.Submit",
                dm: "提交",
              })}
            </Button>
          </div>
        )}
      </Form>
    </div>
  );
};

export default GraphDefinition;
