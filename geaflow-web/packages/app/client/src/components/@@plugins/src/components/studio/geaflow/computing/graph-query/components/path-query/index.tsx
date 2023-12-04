import { CaretRightOutlined, SettingOutlined } from "@ant-design/icons";
import { IUserEdge, IUserNode } from "@antv/graphin";
import { Button, Select } from "antd";
import { filter, find, join, last, map } from "lodash";
import React from "react";
import { useImmer } from "use-immer";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import { PathModal } from "./path-modal";

import styles from "./index.module.less";

const { Option } = Select;
type Prop = {
  edges: Array<IUserEdge>;
  nodes: Array<IUserNode>;
  onQueryPath: (
    limit: number,
    conditions: Array<{ property: string; value: string; operator: string }>,
    path: string
  ) => void;
};
export const PathQueryPanel: React.FC<Prop> = ({
  edges,
  nodes,
  onQueryPath,
}) => {
  const [state, updateState] = useImmer<{
    pathList: Array<Array<IUserEdge>>;
    selectPath: Array<IUserEdge>;
    open: boolean;
    limit: number;
    conditions: Array<{ property: string; value: string; operator: string }>;
  }>({
    pathList: map(edges, (item) => [item]),
    selectPath: [],
    open: false,
    limit: 100,
    conditions: [],
  });
  const { pathList, selectPath, open, limit, conditions } = state;
  const nodeDiv = (n: string) => (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-node`]}>{n}</div>
  );
  const edgeDiv = (r: string) => (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-edge`]}>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-edge-left`]}></div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-edge-canter`]}>{r}</div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-edge-right`]} />
      <CaretRightOutlined />
    </div>
  );

  return (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-path-container`]}>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-path-header`]}>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-header-item`]}>
          <span>路径</span>
          <Select
            placeholder="请选择下列路径"
            allowClear
            onClear={() => {
              updateState((draft) => {
                draft.pathList = map(edges, (item) => [item]);
              });
            }}
            onSelect={(val) => {
              updateState((draft) => {
                const clickItem = last(
                  find(pathList, (item, index) => val === index)
                );
                if (clickItem?.source === clickItem?.target) {
                  draft.pathList = [
                    [
                      ...find(pathList, (item, index) => val === index),
                      last(find(pathList, (item, index) => val === index)),
                    ],
                  ];
                  draft.selectPath = [...pathList[val]];
                } else {
                  const optionPath = filter(
                    edges,
                    (item) => item.source === clickItem?.target
                  );
                  draft.selectPath = [...pathList[val]];
                  draft.pathList = map(optionPath, (item) => [
                    ...find(pathList, (item, index) => val === index),
                    item,
                  ]);
                }
              });
            }}
          >
            {map(pathList, (item, index) => {
              return (
                <Option key={index} value={index}>
                  {map(item, (path, i) => (
                    <div className={styles[`${PUBLIC_PERFIX_CLASS}-option`]}>
                      {i === 0 && nodeDiv(`n${i} | ${path.source}`)}
                      {edgeDiv(`r${i} | ${path.label}`)}
                      {nodeDiv(`n${i + 1} | ${path.target}`)}
                    </div>
                  ))}
                </Option>
              );
            })}
          </Select>
        </div>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-header-item`]}>
          <Button
            onClick={() => {
              updateState((draft) => {
                draft.open = true;
              });
            }}
            type="text"
            icon={<SettingOutlined />}
          >
            高级配置
          </Button>
          <Button
            type="primary"
            onClick={() => {
              const path = map(selectPath, (item, index) => {
                if (index == 0) {
                  return `(n${index}:${item.source})-[r${index}:${
                    item.label
                  }]->(n${index + 1}:${item.target})`;
                }
                return `-[r${index}:${item.label}]->(n${index + 1}:${
                  item.target
                })`;
              });
              onQueryPath(limit, conditions, join(path, ""));
            }}
          >
            执行
          </Button>
        </div>
      </div>
      <PathModal
        data={{ path: selectPath, nodes: nodes }}
        open={open}
        onOK={(params) => {
          updateState((draft) => {
            draft.limit = params.limit;
            draft.conditions = params.conditions;
          });
        }}
        onCancel={() => {
          updateState((draft) => {
            draft.open = false;
          });
        }}
      />
    </div>
  );
};
