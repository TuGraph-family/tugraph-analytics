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

import React, { useEffect, useState } from "react";
import { Line } from '@ant-design/plots';
import { getMetriclist } from "./services";
import styles from "../index.module.less";
import { isEmpty } from "lodash";
import moment from "moment";
interface props {
  startTime: string;
  endTime: string;
  queries: string;
  name?: string;
  taskId?: string;
}

export const GraphArea: React.FC<props> = ({
  startTime,
  endTime,
  queries,
  name,
  taskId,
}) => {
  const [state, setState] = useState({
    lineData: []
  })

  const queryMetricList = async () => {
    const resp = await getMetriclist(taskId as string, {
      start: startTime,
      queries: JSON.parse(queries),
      end: endTime,
    })

    if (!isEmpty(resp)) {
      resp.forEach((d) => {
        d.time = moment(d.time).format("MM-DD HH:mm:ss");
      });
    }

    setState({
      lineData: resp
    })

  }
  useEffect(() => {
    queryMetricList()
  }, [startTime, endTime, queries, taskId]);

  const config = {
    data: state.lineData,
    xField: "time",
    yField: "value",
    seriesField: "metric",
    //  isStack: true,
    legend: {
      position: "top",
    },
    padding: [50],
    smooth: true,
    connectNulls: true,
    // 配置折线趋势填充
    area: {
      style: {
        fillOpacity: 0.15,
      },
    },
    animation: {
      appear: {
        animation: "wave-in",
        duration: 1500,
      },
    },
  }
  return (
    <div className={styles["graph-area"]}>
      <p>{name === "offset" ? "" : name}</p>
      <div style={{ width: "100%", height: 160 }}>
        <Line {...config} />
      </div>
    </div>
  );
};
