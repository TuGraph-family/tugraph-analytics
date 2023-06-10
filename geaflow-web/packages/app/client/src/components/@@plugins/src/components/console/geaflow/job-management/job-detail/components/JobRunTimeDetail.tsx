import { Input, Breadcrumb, Table } from "antd";
import { ProTable } from "@ant-design/pro-components";
import React, { useEffect, useState } from "react";
import { getJobRuntimeList } from "../../../services/job-detail";
import { JobName } from "./JobName";
import moment from "moment";
import styles from "../index.module.less";

const { Search } = Input;

interface JobDetailsProps {
  jobItem: any;
  redirectPath?: any;
}

export const JobRunTimeDetail: React.FC<JobDetailsProps> = ({ jobItem }) => {
  const [state, setState] = useState({
    metricsData: [],
    originList: [],
    name: "",
  });
  const [action, setAction] = useState(null);
  const { id } = jobItem;
  const messageHandle = async () => {
    const respData = await getJobRuntimeList(jobItem);
    setState({
      metricsData: respData?.list,
      originList: respData?.list,
    });
  };

  useEffect(() => {
    messageHandle();
  }, [id]);

  const columns = [
    {
      title: "Job名称",
      dataIndex: "name",
      key: "name",
      render: (text: string, record) => {
        return (
          <a
            onClick={() => {
              setAction(record);
            }}
          >
            {text}
          </a>
        );
      },
    },
    {
      title: "开始时间",
      dataIndex: "startTime",
      key: "startTime",
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
      title: "总耗时",
      dataIndex: "duration",
      key: "duration",
      defaultSortOrder: "descend",
      sorter: (a, b) => a.duration - b.duration,
    },
  ];

  return (
    <div className={styles["job-message"]}>
      {action ? (
        <>
          <Breadcrumb>
            <Breadcrumb.Item>
              <a onClick={() => setAction(null)}>Pipeline 列表</a>
            </Breadcrumb.Item>
            <Breadcrumb.Item>{action.name}</Breadcrumb.Item>
          </Breadcrumb>
          <JobName jobItem={jobItem} pipelineItem={action} />
        </>
      ) : (
        <Table
          className={styles["events-table"]}
          columns={columns}
          dataSource={state.metricsData}
          pagination={{
            pageSize: 10,
          }}
        />
      )}
    </div>
  );
};
