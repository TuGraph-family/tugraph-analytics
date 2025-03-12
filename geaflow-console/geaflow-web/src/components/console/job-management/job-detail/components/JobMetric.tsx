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

import { Button, DatePicker, Form, Tabs } from "antd";
import React, { useEffect, useState } from "react";
import moment from "moment";
import { getMetricMeta } from "./services";
import { GraphArea } from "./graphArea";
import { isEmpty } from "lodash";
import styles from "../index.module.less";
import $i18n from "@/components/i18n";

interface JobMetricProps {
  jobItem: any;
}

export const JobMetric: React.FC<JobMetricProps> = ({ jobItem }) => {
  const handleFormat = (i: any) => {
    return new Date(i)?.getTime();
  };
  const [metricList, setMetircList] = useState({
    metricData: [],
    startTime: handleFormat(moment(Date.now() - 1800000)),
    endTime: handleFormat(moment()),
  });

  const messageHandle = async () => {
    const metricData = await getMetricMeta(jobItem.id);
    if (!isEmpty(metricData)) {
      let arr2 = [];
      for (let i = 0; i < metricData.length; i++) {
        if (arr2.length == 0) {
          arr2.push({
            metricGroup: metricData[i].metricGroup,
            data: [metricData[i]],
          });
        } else {
          const index = arr2.findIndex((item) => {
            return item.metricGroup == metricData[i].metricGroup;
          });
          if (index >= 0) {
            arr2[index].data.push(metricData[i]);
          } else {
            arr2.push({
              metricGroup: metricData[i].metricGroup,
              data: [metricData[i]],
            });
          }
        }
      }
      setMetircList({ ...metricList, metricData: arr2 });
    }
  };

  useEffect(() => {
    messageHandle();
  }, [jobItem]);

  const onFinish = (values: any) => {
    const { start, end } = values;
    setMetircList({
      ...metricList,
      startTime: handleFormat(start),
      endTime: handleFormat(end),
    });
  };

  return (
    <div className={styles["job-metric"]}>
      <div className={styles["metric-header"]}>
        <p style={{ paddingTop: 16, paddingLeft: 42, fontSize: 16 }}>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.JobMetric.SelectTime",
            dm: "选择时间",
          })}
        </p>
        <Form
          name="basic"
          labelCol={{ span: 3 }}
          wrapperCol={{ span: 9 }}
          onFinish={onFinish}
        >
          <Form.Item
            label={$i18n.get({
              id: "openpiece-geaflow.job-detail.components.JobMetric.StartTime",
              dm: "起始时间",
            })}
            name="start"
            rules={[
              {
                required: true,
                message: $i18n.get({
                  id: "openpiece-geaflow.job-detail.components.JobMetric.EnterTheStartTime",
                  dm: "请输入起始时间!",
                }),
              },
            ]}
            initialValue={moment(Date.now() - 1800000)}
          >
            <DatePicker showTime style={{ width: "100%" }} />
          </Form.Item>

          <Form.Item
            label={$i18n.get({
              id: "openpiece-geaflow.job-detail.components.JobMetric.EndTime",
              dm: "结束时间",
            })}
            name="end"
            rules={[
              {
                required: true,
                message: $i18n.get({
                  id: "openpiece-geaflow.job-detail.components.JobMetric.EnterTheEndTime",
                  dm: "请输入结束时间!",
                }),
              },
            ]}
            initialValue={moment()}
          >
            <DatePicker showTime style={{ width: "100%" }} />
          </Form.Item>

          <Form.Item wrapperCol={{ offset: 1, span: 9 }}>
            <Button type="primary" htmlType="submit">
              {$i18n.get({
                id: "openpiece-geaflow.job-detail.components.JobMetric.Update",
                dm: "更新",
              })}
            </Button>
          </Form.Item>
        </Form>
      </div>

      <div className={styles["tabs-body"]}>
        <Tabs>
          {metricList.metricData?.map((item) => {
            return (
              <Tabs.TabPane
                tab={item.metricGroup}
                key={item.metricGroup}
                className={styles["tabs-content"]}
              >
                {item?.data?.map((i) => {
                  return (
                    <GraphArea
                      startTime={metricList.startTime}
                      endTime={metricList.endTime}
                      queries={i?.queries || ""}
                      name={i?.metricName}
                      taskId={jobItem.id}
                    />
                  );
                })}
              </Tabs.TabPane>
            );
          })}
        </Tabs>
      </div>
    </div>
  );
};
