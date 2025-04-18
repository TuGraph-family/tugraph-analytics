/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Input, Table } from "antd";
import React, { useEffect, useState } from "react";
import { ProTable } from "@ant-design/pro-components";
import moment from "moment";
import { getJobOffsetList } from "../../../services/job-detail";
import { convertMillisecondsToHMS } from "../../../util";
import styles from "../index.module.less";
import $i18n from "@/components/i18n";

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
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobOffSet.PartitionName",
        dm: "Partition名称",
      }),
      dataIndex: "partitionName",
      key: "partitionName",
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobOffSet.Latency",
        dm: "延时",
      }),
      dataIndex: "offsetDiff",
      render: (text: number, record) => {
        if (record.type === "TIMESTAMP") {
          return convertMillisecondsToHMS(record.diff);
        }
        return <span>-</span>;
      },
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobOffSet.OffsetDataSourceTime",
        dm: "Offset (数据源时间)",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobOffSet.UpdateTimeServerTime",
        dm: "更新时间 (服务器时间)",
      }),
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
