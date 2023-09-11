import React, { useEffect, useState } from "react";
import { Button, Tooltip } from "antd";
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";
import styles from "../index.module.less";
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
  const jobEditUrl = redirectPath?.find((d) => d.pathName === "图任务");

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
    window.location.href = `${jobEditUrl.path}?jobId=${record.release.job.id}`;
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
