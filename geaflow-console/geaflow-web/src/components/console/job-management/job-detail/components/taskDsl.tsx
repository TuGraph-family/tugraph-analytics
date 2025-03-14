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
