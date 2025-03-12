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

import React from "react";
import { GraphDefinitionConfigPanel } from "./graphDefinitionConfigPanel";
import { Form } from "antd";
import { isEmpty } from "lodash";

interface ClusterConfigProps {
  record: any;
  stageType: string;
  syncConfig: (params: any) => void;
  form: any;
}

const ClusterConfig: React.FC<ClusterConfigProps> = ({
  record,
  stageType,
  form,
}) => {
  let defaultFormValues = {};
  // 根据 currentItem 来判断是新增还是修改
  if (!isEmpty(record?.release?.clusterConfig)) {
    const configArr = [];
    for (const key in record?.release?.clusterConfig) {
      const current = record?.release?.clusterConfig[key];
      configArr.push({
        key,
        value: current,
      });
    }

    defaultFormValues = {
      clusterConfig: {
        type: record?.type,
        config: configArr,
      },
    };
  } else {
    defaultFormValues = {
      clusterConfig: {
        config: [],
      },
    };
  }

  return (
    <div>
      <Form initialValues={defaultFormValues} form={form}>
        <GraphDefinitionConfigPanel
          prefixName="clusterConfig"
          form={form}
          readonly={stageType !== "CREATED"}
        />
      </Form>
    </div>
  );
};

export default ClusterConfig;
