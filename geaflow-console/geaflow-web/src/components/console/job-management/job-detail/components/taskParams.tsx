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
import { Button, Tooltip } from "antd";
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";
import styles from "../index.module.less";
import { history } from "umi";
import { useTranslation } from "react-i18next";

interface TaskParamsProps {
  record: any;
  syncConfig: (params: any) => void;
  redirectPath?: any[];
}

const TaskParams: React.FC<TaskParamsProps> = ({
  record,
  syncConfig,
  redirectPath,
}) => {
  const [state, setState] = useState<{
    mainClass: string;
    taskConfig: string;
  }>({
    mainClass: record?.mainClass,
    taskConfig: "",
  });

  const { t } = useTranslation();

  useEffect(() => {
    if (record) {
      setState({ ...state, taskConfig: record?.release?.job?.userCode });
    }
  }, [record]);

  const handleCodeChange = (value: string) => {
    setState({
      ...state,
      taskConfig: value,
    });
    syncConfig({
      taskConfig: value,
    });
  };

  const handleToEditPage = () => {
    history.push(`/studio/StudioComputing?jobId=${record?.release?.job.id}`);
  };
  return (
    <div className={styles["task-params"]}>
      <Tooltip title={t("i18n.key.jump.task.editpage")}>
        <Button
          style={{
            position: "absolute",
            zIndex: 2,
            right: 0,
            top: -1,
          }}
          size="small"
          onClick={handleToEditPage}
        >
          {t("i18n.key.Edit")}
        </Button>
      </Tooltip>
      <CodeMirror
        value={state.taskConfig}
        extensions={[json()]}
        onChange={handleCodeChange}
        readOnly={true}
      />
    </div>
  );
};

export default TaskParams;
