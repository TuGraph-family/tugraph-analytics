import { Input, Table } from "antd";
import { ProTable } from "@ant-design/pro-components";
import React, { useEffect, useState } from "react";
import moment from "moment";
import { getPipleinesCyclesByName } from "../../../services/job-detail";
import styles from "../index.module.less";

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
      title: "cycleName",
      dataIndex: "name",
      key: "name",
      width: 80,
      hideInSearch: true,
    },

    {
      title: "开始时间",
      dataIndex: "startTime",
      key: "startTime",
      width: 120,
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
      title: "opName",
      dataIndex: "opName",
      key: "opName",
      width: 80,
      hideInSearch: true,
    },
    {
      title: "总耗时",
      dataIndex: "duration",
      key: "duration",
      hideInSearch: true,
      width: 80,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.duration - b.duration,
    },
    {
      title: "平均耗时",
      dataIndex: "avgExecuteTime",
      key: "avgExecuteTime",
      hideInSearch: true,
      width: 80,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.avgExecuteTime - b.avgExecuteTime,
    },
    {
      title: "平均GC时间",
      dataIndex: "avgGcTime",
      key: "avgGcTime",
      hideInSearch: true,
      width: 80,
    },
    {
      title: "task个数",
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
      title: "最慢task(id/耗时)",
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
