import React from 'react';
import { GraphDefinitionConfigPanel } from './graphDefinitionConfigPanel';
import { Form } from 'antd';
import { isEmpty } from 'lodash';

interface ClusterConfigProps {
  record: any;
  stageType: string;
  syncConfig: (params: any) => void;
  form: any;
}

const userCode: React.FC<ClusterConfigProps> = ({ record, stageType, form }) => {
  let defaultFormValues = {};
  // 根据 currentItem 来判断是新增还是修改
  if (!isEmpty(record?.release?.jobConfig)) {
    const configArr = [];
    for (const key in record?.release?.jobConfig) {
      const current = record?.release?.jobConfig[key];
      configArr.push({
        key,
        value: current,
      });
    }

    defaultFormValues = {
      jobConfig: {
        type: record?.type,
        config: configArr,
      },
    };
  } else {
    defaultFormValues = {
      jobConfig: {
        config: [],
      },
    };
  }

  return (
    <div>
      <Form initialValues={defaultFormValues} form={form}>
        <GraphDefinitionConfigPanel prefixName="jobConfig" form={form} readonly={stageType !== 'CREATED'} />
      </Form>
    </div>
  );
};

export default userCode;
