import React from "react";
import { Tabs } from "antd";
import TaskParams from "./taskParams";
import TaskDsl from "./taskDsl";
import ClusterConfig from "./clusterConfig";
import UserCode from "./userCode";
import { isEmpty } from "lodash";

interface BasicTabsProps {
  record: any;
  jobItem: any;
  stageType: string;
  syncConfig: (params: any) => void;
  form: any;
}

const BasicTabs: React.FC<BasicTabsProps> = ({
  record,
  jobItem,
  stageType,
  syncConfig,
  form,
}) => {
  const { dsl = "" } = jobItem;

  const items = [
    {
      label: `用户代码`,
      key: "1",
      children: (
        <TaskParams
          syncConfig={syncConfig}
          record={record}
          stageType={stageType}
        />
      ),
    },
    {
      label: `任务参数`,
      key: "2",
      children: (
        <UserCode
          syncConfig={syncConfig}
          record={record}
          stageType={stageType}
          form={form}
        />
      ),
    },
    {
      label: `集群参数`,
      key: "3",
      children: (
        <ClusterConfig
          syncConfig={syncConfig}
          record={record}
          stageType={stageType}
          form={form}
        />
      ),
    },

    ...(!isEmpty(dsl)
      ? [
          {
            label: `dsl`,
            key: "e",
            children: (
              <TaskDsl
                syncConfig={syncConfig}
                record={record}
                stageType={stageType}
              />
            ),
          },
        ]
      : []),
  ];

  return (
    <Tabs defaultActiveKey="1">
      {items.map((item) => {
        return (
          <Tabs.TabPane tab={item.label} key={item.key}>
            {item.children}
          </Tabs.TabPane>
        );
      })}
    </Tabs>
  );
};

export default BasicTabs;
