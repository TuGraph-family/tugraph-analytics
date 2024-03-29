import React from "react";
import { Tabs } from "antd";
import TaskParams from "./taskParams";
import TaskDsl from "./taskDsl";
import UDFList from "./udfList";
import ClusterConfig from "./clusterConfig";
import UserCode from "./userCode";
import { isEmpty } from "lodash";
import $i18n from "@/components/i18n";
import { GraphDefintionTab } from "@/components/studio/computing/graph-tabs";

interface BasicTabsProps {
  record: any;
  jobItem: any;
  stageType: string;
  syncConfig: (params: any) => void;
  form: any;
  redirectPath?: any[];
}

const BasicTabs: React.FC<BasicTabsProps> = ({
  record,
  jobItem,
  stageType,
  syncConfig,
  form,
  redirectPath,
}) => {
  const { dsl = "" } = jobItem;
  const structMappings =
    !isEmpty(jobItem.release.job.structMappings) &&
    JSON.parse(jobItem.release.job.structMappings);
  const jobType = record?.release?.job?.type;
  const items = [
    ...(jobType !== "SERVE"
      ? [
          {
            label:
              jobType === "CUSTOM"
                ? $i18n.get({
                    id: "openpiece-geaflow.job-detail.components.basicTabs.Udf",
                    dm: "UDF",
                  })
                : $i18n.get({
                    id: "openpiece-geaflow.job-detail.components.basicTabs.UserCode",
                    dm: "用户代码",
                  }),
            key: "1",
            children:
              jobType === "CUSTOM" ? (
                <UDFList syncConfig={syncConfig} record={record} />
              ) : (
                <TaskParams
                  syncConfig={syncConfig}
                  record={record}
                  redirectPath={redirectPath}
                />
              ),
          },
        ]
      : []),
    ...(jobType === "INTEGRATE"
      ? [
          {
            label: $i18n.get({
              id: "openpiece-geaflow.job-detail.components.basicTabs.StructMapping",
              dm: "数据映射",
            }),
            key: "4",
            children: (
              <GraphDefintionTab
                form={form}
                serveList={jobItem.release.job.graphs}
                tableList={[]}
                fields={structMappings}
                check={true}
              />
            ),
          },
        ]
      : []),
    {
      label: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.basicTabs.TaskParameters",
        dm: "任务参数",
      }),
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
      label: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.basicTabs.ClusterParameters",
        dm: "集群参数",
      }),
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
