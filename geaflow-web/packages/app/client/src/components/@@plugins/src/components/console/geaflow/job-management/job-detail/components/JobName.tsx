import { Input, Table } from "antd";
import { ProTable } from "@ant-design/pro-components";
import React, { useEffect, useState } from "react";
import moment from "moment";
import { getPipleinesCyclesByName } from "../../../services/job-detail";
import styles from "../index.module.less";
import $i18n from "../../../../../../../../i18n";

const { Search } = Input;

interface JobJournalProps {
  jobItem: any;
  pipelineItem: any;
}

export const JobName: React.FC<JobJournalProps> = ({
  jobItem,
  pipelineItem,
}) => {
  const [state, setState] = useState({
    currentList: [],
    originList: [],
  });

  const { id } = jobItem;
  const messageHandle = async () => {
    if (id && pipelineItem) {
      const respData = await getPipleinesCyclesByName({
        id,
        name: pipelineItem.name,
      });

      if (respData) {
        setState({
          currentList: respData.list,
          originList: respData.list,
        });
      }
    }
  };

  useEffect(() => {
    messageHandle();
  }, [id, pipelineItem?.name]);

  const stagesColumns = [
    {
      title: "Cycle Name",
      dataIndex: "name",
      key: "name",
      width: 120,
      hideInSearch: true,
    },

    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobName.StartTime",
        dm: "开始时间",
      }),
      dataIndex: "startTime",
      key: "startTime",
      width: 80,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.startTime - b.startTime,
      render: (text: number) => {
        if (!text) {
          return "-";
        }
        return moment(text).format("YYYY-MM-DD HH:mm:ss");
      },
    },
    {
      title: "Operator Name",
      dataIndex: "opName",
      key: "opName",
      width: 80,
      hideInSearch: true,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobName.TotalTimeConsumption",
        dm: "总耗时",
      }),
      dataIndex: "duration",
      key: "duration",
      hideInSearch: true,
      width: 80,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.duration - b.duration,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobName.AverageTimeConsumption",
        dm: "平均耗时",
      }),
      dataIndex: "avgExecuteTime",
      key: "avgExecuteTime",
      hideInSearch: true,
      width: 80,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.avgExecuteTime - b.avgExecuteTime,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobName.AverageGcTime",
        dm: "平均GC时间",
      }),
      dataIndex: "avgGcTime",
      key: "avgGcTime",
      hideInSearch: true,
      width: 80,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobName.NumberOfTasks",
        dm: "task个数",
      }),
      dataIndex: "totalTasks",
      key: "totalTasks",
      width: 80,
      hideInSearch: true,
    },
    {
      title: "in(num/kb)",
      key: "inputRecords",
      hideInSearch: true,
      width: 80,
      render: (_, record) => (
        <span>{record.inputRecords + " / " + record.inputKb}</span>
      ),
    },
    {
      title: "out(num/kb)",
      key: "outputKb: 0",
      width: 80,
      hideInSearch: true,
      render: (_, record) => (
        <span>{record.outputKb + " / " + record.outputKb}</span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobName.SlowestTaskIdTimeConsuming",
        dm: "最慢task(id/耗时)",
      }),
      key: "avgExecuteTime",
      hideInSearch: true,
      width: 80,
      render: (_, record) => (
        <span>
          {record.slowestTask + " / " + record.slowestTaskExecuteTime}
        </span>
      ),
    },
  ];

  return (
    <div className={styles["job-journal"]}>
      <Table
        style={{ marginTop: 16 }}
        columns={stagesColumns}
        dataSource={state.currentList}
        pagination={{
          pageSize: 20,
          showSizeChanger: true,
          showQuickJumper: true,
        }}
      />
    </div>
  );
};
