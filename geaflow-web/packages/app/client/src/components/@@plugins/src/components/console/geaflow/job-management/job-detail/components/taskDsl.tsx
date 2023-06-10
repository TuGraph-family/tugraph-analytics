import React, { useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { json } from '@codemirror/lang-json';

interface TaskParamsProps {
  record: any;
  stageType: string;
  syncConfig: (params: any) => void;
}

const TaskDsl: React.FC<TaskParamsProps> = ({ record, stageType, syncConfig }) => {
  const [state, setState] = useState<{
    mainClass: string;
    dslConfig: string;
  }>({
    mainClass: record.mainClass,
    dslConfig: record?.dsl,
  });

  const handleCodeChange = (value: string) => {
    setState({
      ...state,
      dslConfig: value,
    });
    syncConfig({
      dslConfig: value,
    });
  };
  return (
    <div>
      <CodeMirror
        value={state.dslConfig}
        extensions={[json()]}
        onChange={handleCodeChange}
        readOnly={stageType !== 'CREATED'}
      />
    </div>
  );
};

export default TaskDsl;
