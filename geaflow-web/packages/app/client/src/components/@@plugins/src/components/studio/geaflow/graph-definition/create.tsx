import React, { useState, useEffect } from "react";
import { Input, Row, Col, Form, Card, Button, message } from "antd";
import {
  createGraphDefinition,
  updateGraphDefinition,
  graphDetail,
} from "../services/graphDefinition";
import { GraphDefintionTab } from "./graph-tabs";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";
import { isEmpty } from "lodash";

const GraphDefinition = ({ graphName, toBackList, readonly, editable }) => {
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const { value: instanceName } = currentInstance;

  const [form] = Form.useForm();
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [topology, setTopology] = useState({
    vertices: [],
    edges: [],
  });
  const [currentItem, setCurrentItem] = useState<object>({});
  const [activeKey, setActiveKey] = useState<string>("VERTEX");
  useEffect(() => {
    if (graphName) {
      graphDetail(instanceName, graphName).then((res) => {
        if (res.success) {
          setCurrentItem(res.data);
          form.setFieldsValue({
            name: res.data.name,
            comment: res.data?.comment,
          });
          let vertices = [];
          res.data?.vertices.forEach((item) => {
            vertices.push(item.id);
          });
          let edges = [];
          res.data?.edges.forEach((item) => {
            edges.push(item.id);
          });
          setTopology({ vertices, edges });
        }
      });
    }
  }, [graphName]);

  const onSave = async () => {
    const values = await form.validateFields();
    setIsLoading(true);
    // 参数配置的值
    const vertices = topology.vertices.map((item) => {
      return { id: item };
    });

    const edges = topology.edges.map((item) => {
      return { id: item };
    });
    const { pluginConfig = {}, name, comment } = values;
    const endpoints = Object.values(values)?.filter(
      (str) => str && Object.keys(str).indexOf("edgeName") !== -1
    );
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
        edges,
        vertices,
        endpoints,
        pluginConfig: {
          ...currentItem?.pluginConfig,
          type: type || currentItem?.pluginConfig.type,
          config:
            config.length > 0
              ? configObj.config
              : currentItem?.pluginConfig.config,
        },
      };
      const updateResult = await updateGraphDefinition(
        instanceName,
        graphName,
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
    if (configObj.type) {
      const createParams = {
        name,
        vertices,
        edges,
        endpoints,
        comment,
        pluginConfig: configObj,
      };

      const result = await createGraphDefinition(instanceName, createParams);

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
    } else {
      message.info(
        $i18n.get({
          id: "openpiece-geaflow.geaflow.table-definition.create.EnterParameterConfiguration",
          dm: "请选择参数配置",
        })
      );
      setActiveKey("paramConfig");
    }
    setIsLoading(false);
  };

  let defaultFormValues = {};

  // 根据 currentItem 来判断是新增还是修改
  if (!isEmpty(currentItem)) {
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
                dm: "拓扑配置",
              }),
              type: "VERTEX",
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
          topology={topology}
          setTopology={setTopology}
          activeKey={activeKey}
          setActiveKey={setActiveKey}
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
