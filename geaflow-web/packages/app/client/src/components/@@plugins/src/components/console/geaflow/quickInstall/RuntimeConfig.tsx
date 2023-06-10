import React from 'react'
import { Collapse, FormInstance, Alert } from 'antd'
import { CommonConfig } from './CommonConfig';
import { IDefaultValues } from './index';
const { Panel } = Collapse;

interface IProps {
  values: {
    runtimeMetaConfig: IDefaultValues;
    haMetaConfig: IDefaultValues;
    metricConfig: IDefaultValues;
  };
  form: FormInstance;
}

export const RuntimeConfig: React.FC<IProps> = ({ values, form }) => {
  return <>
    <Alert message='配置GeaFlow作业默认元数据存储，含运行时元数据、HA元数据、指标数据。单机部署模式下，你可以使用默认配置，容器会自动拉起本地MySQL、Redis和TSDB服务。' type="info" showIcon style={{ marginBottom: 24 }} />
    <Collapse defaultActiveKey={['runtimeMetaConfig', 'haMetaConfig', 'metricConfig']}>
      <Panel header={values.runtimeMetaConfig?.comment || '运行时元数据配置'} key='runtimeMetaConfig'>
        <CommonConfig prefixName='runtimeMetaConfig' values={values.runtimeMetaConfig} form={form} />
      </Panel>
      <Panel header={values.haMetaConfig?.comment || 'HA元数据存储'} key='haMetaConfig'>
        <CommonConfig prefixName='haMetaConfig' values={values.haMetaConfig} form={form} />
      </Panel>
      <Panel header={values.metricConfig?.comment || '指标存储'} key='metricConfig'>
      <CommonConfig prefixName='metricConfig' values={values.metricConfig} form={form} />
      </Panel>
    </Collapse>
  </>
}
