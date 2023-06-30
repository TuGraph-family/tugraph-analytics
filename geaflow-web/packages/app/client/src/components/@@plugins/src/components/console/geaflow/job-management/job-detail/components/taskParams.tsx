import React, { useEffect, useState } from "react";
import { Button, Alert } from "antd";
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

  const handleCodeChange = (value: string) => {
    setState({
      ...state,
      taskConfig: value,
    });
    syncConfig({
      taskConfig: value,
    });
  };

  const handleToEdit = () => {
    console.log('record', record)
  }
  return (
    <div>
      <Button 
        type='primary'
        style={{ position: 'absolute', zIndex: 3, right: 0 }}
        onClick={handleToEdit}>编辑</Button>
      <Alert message={<>编辑用户代码，需要在图计算编辑，点击去<Button type="link">编辑</Button></>} />
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
