import React, { useEffect, useState } from "react";
import { Input } from "antd";
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";

interface TaskParamsProps {
  record: any;
  stageType: string;
  syncConfig: (params: any) => void;
}

const TaskParams: React.FC<TaskParamsProps> = ({
  record,
  stageType,
  syncConfig,
}) => {
  const [state, setState] = useState<{
    mainClass: string;
    taskConfig: string;
  }>({
    mainClass: record?.mainClass,
    taskConfig: "",
  });

  useEffect(() => {
    if (record) {
      setState({ ...state, taskConfig: record?.release?.job?.userCode });
    }
  }, [record]);

  const handleValueChange = (evt) => {
    setState({
      ...state,
      mainClass: evt.target.value,
    });
    syncConfig({
      mainClass: evt.target.value,
    });
  };

  const handleCodeChange = (value: string) => {
    setState({
      ...state,
      taskConfig: value,
    });
    syncConfig({
      taskConfig: value,
    });
  };
  return (
    <div>
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
