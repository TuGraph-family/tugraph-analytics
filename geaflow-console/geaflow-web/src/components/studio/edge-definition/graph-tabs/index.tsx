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

import { Button, Tabs, Popconfirm } from "antd";
import React, { useState, useEffect } from "react";
import { PlusOutlined } from "@ant-design/icons";
import { GraphDefintionEditTable } from "./graphDefintionEditTable";
import { DeleteOutlined } from "@ant-design/icons";
import { isEmpty } from "lodash";
import styles from "./index.less";
import $i18n from "@/components/i18n";

type Props = {
  tabsList: {
    name: string;
    type: string;
    editTables: any;
  }[];
  form: any;
  currentItem?: any;
  readonly: boolean;
  editable?: boolean;
};

const { TabPane } = Tabs;
export const GraphDefintionTab: React.FC<Props> = ({
  tabsList = [],
  form,
  currentItem,
  readonly,
  editable,
}) => {
  const [tabsData, setTabsData] = useState(tabsList);

  useEffect(() => {
    if (!isEmpty(currentItem)) {
      let TableData = tabsData;
      TableData[0].editTables = [currentItem];
      setTabsData([...TableData]);
    }
  }, [currentItem]);

  return (
    <Tabs className={styles["graph-tab"]}>
      {tabsData.map((item, index) => (
        <TabPane tab={item.name} key={item.type}>
          {item.editTables?.map((i, paneIndex) => {
            return (
              <GraphDefintionEditTable
                type={item.type}
                length={0}
                id={i?.id}
                fields={i?.fields}
                index={index}
                paneIndex={paneIndex}
                form={form}
                name={i?.name}
                readonly={readonly}
                editable={editable}
              />
            );
          })}
        </TabPane>
      ))}
    </Tabs>
  );
};
