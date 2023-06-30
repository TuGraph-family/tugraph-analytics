import { Input, Table } from "antd";
import React, { useEffect, useState } from "react";
import { ProTable } from "@ant-design/pro-components";
import moment from "moment";
import { getJobOffsetList } from "../../../services/job-detail";
import { convertMillisecondsToHMS } from "../../../util";
import styles from "../index.module.less";

const { Search } = Input;

interface JobMetricProps {
  jobItem: any;
}

export const JobOffSet: React.FC<JobMetricProps> = ({ jobItem }) => {
  const { id } = jobItem;

  const [state, setState] = useState({
    offsetData: [],
    originOffsetData: [],
  });

  const messageHandle = async () => {
    const respData = await getJobOffsetList(id);

    setState({
      ...state,
      offsetData: respData,
      originOffsetData: respData,
    });
  };

  useEffect(() => {
    messageHandle();
  }, [id]);

  const columns = [
    {
      title: "Partition名称",
      dataIndex: "partitionName",
      key: "partitionName",
    },
    {
      title: "延时",
      dataIndex: "offsetDiff",
      render: (text: number, record) => {
        if (record.type === "TIMESTAMP") {
          return convertMillisecondsToHMS(record.diff);
        }
        return <span>-</span>;
      },
    },
    {
      title: "Offset",
      dataIndex: "offset",
      key: "offset",
      render: (text: number, record) => {
        if (record.type === "TIMESTAMP") {
          return moment(text).format("YYYY-MM-DD HH:mm:ss");
        }
        return <span>{text}</span>;
      },
    },
    {
      title: "更新时间 (服务器时间)",
      dataIndex: "writeTime",
      render: (text: number) => {
        if (!text) {
          return "-";
        }
        return moment(text).format("YYYY-MM-DD HH:mm:ss");
      },
    },
  ];

  return (
    <div className={styles["job-offset"]}>
      <Table
        columns={columns}
        dataSource={state.offsetData}
        pagination={{
          pageSize: 10,
        }}
      />
    </div>
  );
};
