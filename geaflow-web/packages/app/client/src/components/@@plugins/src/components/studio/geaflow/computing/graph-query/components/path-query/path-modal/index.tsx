import { IUserEdge, IUserNode } from "@antv/graphin";
import {
  Col,
  Form,
  Input,
  InputNumber,
  Modal,
  ModalProps,
  Row,
  Select,
  Tabs,
} from "antd";
import { filter, find, flatMapDeep, map, toArray } from "lodash";
import React, { ReactNode, useEffect } from "react";
import { useImmer } from "use-immer";
import { PUBLIC_PERFIX_CLASS } from "../../../../constant";
import { getConnectOptions } from "../../../utils/getConnectOptions";

import styles from "./index.module.less";

type PathModalProp = {
  data: { path: Array<IUserEdge>; nodes: Array<IUserNode> };
  open: boolean;
  onCancel: () => void;
  onOK: (params: {
    limit: number;
    conditions: Array<{ property: string; value: string; operator: string }>;
  }) => void;
} & ModalProps;
const { Item } = Form;
export const PathModal: React.FC<PathModalProp> = ({
  data,
  open,
  onCancel,
  onOK,
  ...prop
}) => {
  const [form] = Form.useForm();
  const { path = [], nodes = [] } = data;
  const [state, updateState] = useImmer<{
    tabList: Array<{
      label: string;
      key: string;
      children: ReactNode;
      name: string;
      properties: any;
      id: string;
    }>;
  }>({
    tabList: [],
  });
  const { tabList } = state;
  useEffect(() => {
    updateState((draft) => {
      draft.tabList = flatMapDeep(
        map(path, (item, index) => {
          return [
            ...(index === 0
              ? [
                  {
                    ...find(nodes, (node) => node.labelName === item.source),
                    name: `n${index} | ${item.source}`,
                    id: `n${index}`,
                  },
                ]
              : []),
            {
              ...item,
              name: `r${index} | ${item.label}`,
              id: `r${index}`,
            },
            {
              ...find(nodes, (node) => node.labelName === item.target),
              name: `n${index + 1} | ${item.target}`,
              id: `n${index + 1}`,
            },
          ];
        })
      );
    });
  }, [path]);
  return (
    <Modal
      width={978}
      className={styles[`${PUBLIC_PERFIX_CLASS}-path-modal`]}
      title="高级配置"
      visible={open}
      onOk={() => {
        form.validateFields().then((val) => {
          const { limit } = val;
          const conditions = filter(
            flatMapDeep(map(tabList, (item) => toArray(val[item.name]))),
            (item) => item.operator && item.value
          );
          onOK({ limit, conditions });
          onCancel();
        });
      }}
      onCancel={onCancel}
      {...prop}
    >
      <Form form={form}>
        <Item
          rules={[{ required: true, message: "请设置路径数目" }]}
          label="路径数目"
          name="limit"
          initialValue={100}
        >
          <InputNumber placeholder="请输入路径数目" />
        </Item>
        <Tabs>
          {map(tabList, (item) => (
            <Tabs.TabPane key={item.name} tab={item.name}>
              <Row>
                {item.properties?.length ? (
                  map(item.properties, (propey) => (
                    <Col span={12}>
                      <Item
                        name={[item.name, propey.name, "property"]}
                        initialValue={`${item.id}.${propey.name}`}
                        label={propey.name}
                      />
                      <Input.Group compact>
                        <Item name={[item.name, propey.name, "operator"]}>
                          <Select
                            className={
                              styles[`${PUBLIC_PERFIX_CLASS}-div-select`]
                            }
                            placeholder="选择关系"
                            options={getConnectOptions(propey.type)}
                          />
                        </Item>
                        <Item name={[item.name, propey.name, "value"]}>
                          {propey.type === "BOOL" ? (
                            <Select
                              className={
                                styles[`${PUBLIC_PERFIX_CLASS}-div-ipt`]
                              }
                            >
                              <Select.Option value={true}>是</Select.Option>
                              <Select.Option value={false}>否</Select.Option>
                            </Select>
                          ) : (
                            <Input
                              className={
                                styles[`${PUBLIC_PERFIX_CLASS}-div-ipt`]
                              }
                            />
                          )}
                        </Item>
                      </Input.Group>
                    </Col>
                  ))
                ) : (
                  <div className={styles[`${PUBLIC_PERFIX_CLASS}-empty`]}>
                    无可配置项
                  </div>
                )}
              </Row>
            </Tabs.TabPane>
          ))}
        </Tabs>
      </Form>
    </Modal>
  );
};
