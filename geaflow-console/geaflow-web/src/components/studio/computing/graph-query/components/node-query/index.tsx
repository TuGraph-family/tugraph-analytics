import { LeftOutlined, RightOutlined } from "@ant-design/icons";
import { Button, Form, Input, InputNumber, Select, Tabs } from "antd";
import { filter, find, flatMapDeep, map, toArray } from "lodash";
import React from "react";
import { useImmer } from "use-immer";
import SwitchDrawer from "../switch-drawer";
import { PROPERTY_TYPE, PUBLIC_PERFIX_CLASS } from "../../../constant";
import { getConnectOptions } from "../../utils/getConnectOptions";

import styles from "./index.module.less";

const { Item } = Form;
const { Option } = Select;
type NodeProp = {
  indexs: string;
  labelName: string;
  nodeType: string;
  primary: string;
  properties: Array<{ id: string; name: string; type: string }>;
};
type Prop = {
  nodes: Array<NodeProp>;
  nodeQuery: (limit: number, conditions: any, nodes: Array<string>) => void;
};
export const NodeQuery: React.FC<Prop> = ({ nodes, nodeQuery }) => {
  const visible = true;
  const [state, updateState] = useImmer<{
    nodeCheckedList: Array<string>;
    activeKey: string;
    isShow: boolean;
  }>({
    nodeCheckedList: [],
    activeKey: "",
    isShow: false,
  });
  const { nodeCheckedList, activeKey, isShow } = state;
  const [form] = Form.useForm();
  const [nodeForm] = Form.useForm();
  const nodeChange = (list: string[]) => {
    updateState((draft) => {
      draft.indeterminate = !!list.length && list.length < nodes.length;
      draft.nodeCheckedList = [...list];
      draft.activeKey = [...list][0];
    });
  };
  const handleNodeQuery = () => {
    form.validateFields().then((val) => {
      const { limit } = val;
      nodeForm.validateFields().then((nodeVal) => {
        const conditions = filter(
          flatMapDeep(map(nodeCheckedList, (item) => toArray(nodeVal[item]))),
          (condition) => condition.operator || condition.value
        );
        nodeQuery(limit, conditions, nodeCheckedList);
      });
    });
  };
  return (
    <div
      className={`${styles[`${PUBLIC_PERFIX_CLASS}-nodequery`]} ${
        !visible ? `${styles[`${PUBLIC_PERFIX_CLASS}-nodequery-ani`]}` : ""
      }`}
    >
      <SwitchDrawer
        visible={visible}
        // onShow={onShow}
        // onClose={onClose}
        position="left"
        className={styles[`${PUBLIC_PERFIX_CLASS}-nodequery-drawer`]}
        width={280}
        backgroundColor="#f6f6f6"
        footer={
          <>
            <Button
              style={{ marginRight: 8 }}
              onClick={() => {
                nodeForm.resetFields();
              }}
            >
              重置
            </Button>
            <Button
              type="primary"
              onClick={() => {
                handleNodeQuery();
              }}
            >
              执行
            </Button>
          </>
        }
      >
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-drawer-ccontainer`]}>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-drawer-title`]}>
            点查询
          </div>
          <Form layout="vertical" form={form}>
            <div>返回点数目</div>
            <Item
              name="limit"
              rules={[{ required: true, message: "请输入返回点数" }]}
              initialValue={1}
            >
              <InputNumber placeholder="请输入点数目" />
            </Item>
            <Item
              required
              label={"选择点"}
              name="selectNode"
              rules={[{ required: true, message: "请选择点" }]}
            >
              <Select
                mode="multiple"
                options={map(nodes, (item) => ({ value: item.labelName }))}
                value={nodeCheckedList}
                onChange={nodeChange}
                maxTagCount={"responsive"}
              />
            </Item>
            <Tabs
              onMouseEnter={() => {
                updateState((draft) => {
                  draft.isShow = true;
                });
              }}
              onMouseLeave={() => {
                updateState((draft) => {
                  draft.isShow = false;
                });
              }}
              activeKey={activeKey}
              onChange={(val) => {
                updateState((draft) => {
                  draft.activeKey = val;
                });
              }}
            >
              {map(nodeCheckedList, (item, index) => (
                <Tabs.TabPane tab={item} key={item}>
                  {nodeCheckedList.length > 1 && isShow && (
                    <>
                      <div
                        style={{
                          top: find(nodes, (node) => node.labelName === item)
                            ?.properties?.length
                            ? -50
                            : -35,
                        }}
                        className={
                          styles[`${PUBLIC_PERFIX_CLASS}-tab-left-btn`]
                        }
                        onClick={() => {
                          updateState((draft) => {
                            if (index > 0) {
                              draft.activeKey = nodeCheckedList[index - 1];
                            }
                          });
                        }}
                      >
                        <LeftOutlined />
                      </div>
                      <div
                        className={
                          styles[`${PUBLIC_PERFIX_CLASS}-tab-right-btn`]
                        }
                        style={{
                          top: find(nodes, (node) => node.labelName === item)
                            ?.properties?.length
                            ? -50
                            : -35,
                        }}
                        onClick={() => {
                          updateState((draft) => {
                            if (index + 1 < nodeCheckedList.length) {
                              draft.activeKey = nodeCheckedList[index + 1];
                            }
                          });
                        }}
                      >
                        <RightOutlined />
                      </div>
                    </>
                  )}

                  <Form form={nodeForm}>
                    {map(
                      find(nodes, (node) => node.labelName === item)
                        ?.properties,
                      (proper) => (
                        <div>
                          <div
                            style={{
                              lineHeight: "22px",
                              margin: "16px 0 8px 0",
                              color: "rgba(54,55,64,1)",
                            }}
                          >
                            {proper.name}
                          </div>
                          <Item
                            name={[item, proper.name, "property"]}
                            className={
                              styles[
                                `${PUBLIC_PERFIX_CLASS}-property-container`
                              ]
                            }
                            initialValue={`n${index}.${proper.name}`}
                          />
                          <Input.Group compact>
                            <Item
                              name={[item, proper.name, "operator"]}
                              className={
                                styles[
                                  `${PUBLIC_PERFIX_CLASS}-select-container`
                                ]
                              }
                            >
                              <Select
                                placeholder="选择关系"
                                options={getConnectOptions(proper.type)}
                              />
                            </Item>
                            <Item
                              name={[item, proper.name, "value"]}
                              className={
                                styles[`${PUBLIC_PERFIX_CLASS}-input-container`]
                              }
                            >
                              {PROPERTY_TYPE[proper.type] === "number" ? (
                                <InputNumber />
                              ) : PROPERTY_TYPE[proper.type] === "string" ? (
                                <Input />
                              ) : (
                                <Select>
                                  <Select.Option value={true}>是</Select.Option>
                                  <Select.Option value={false}>
                                    否
                                  </Select.Option>
                                </Select>
                              )}
                            </Item>
                          </Input.Group>
                        </div>
                      )
                    )}
                  </Form>
                </Tabs.TabPane>
              ))}
            </Tabs>
          </Form>
        </div>
      </SwitchDrawer>
    </div>
  );
};
