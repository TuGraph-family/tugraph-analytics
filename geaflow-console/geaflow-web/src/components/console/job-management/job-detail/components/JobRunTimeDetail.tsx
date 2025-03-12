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

import { Input, Breadcrumb, Table } from "antd";
import { ProTable } from "@ant-design/pro-components";
import React, { useEffect, useState } from "react";
import { getJobRuntimeList } from "../../../services/job-detail";
import { JobName } from "./JobName";
import moment from "moment";
import styles from "../index.module.less";
import $i18n from "@/components/i18n";

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
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobRunTimeDetail.PipelineName",
        dm: "Job名称",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobRunTimeDetail.StartTime",
        dm: "开始时间",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobRunTimeDetail.TotalTimeConsumption",
        dm: "总耗时",
      }),
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
              <a onClick={() => setAction(null)}>
                {$i18n.get({
                  id: "openpiece-geaflow.job-detail.components.JobRunTimeDetail.PipelineList",
                  dm: "Pipeline 列表",
                })}
              </a>
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
