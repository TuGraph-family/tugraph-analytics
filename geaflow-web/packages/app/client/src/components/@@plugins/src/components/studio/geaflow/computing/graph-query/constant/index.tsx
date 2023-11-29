import { GraphinData } from '@antv/graphin';
import { Tooltip } from 'antd';
import type { ColumnsType } from 'antd/lib/table';
import React from 'react';

export const EXCECUTE_EFFICIENCY_RESULT_TABLE: ColumnsType<any> = [
  {
    dataIndex: 'step',
    title: 'Step',
  },
  {
    dataIndex: 'count',
    title: 'Count',
    width: 200,
  },
  {
    dataIndex: 'traversers',
    title: 'Traversers',
    width: 200,
  },
  {
    dataIndex: 'time',
    title: 'Time',
    width: 200,
  },
  {
    dataIndex: 'dur',
    title: 'Dur',
    width: 200,
  },
];
export const EXCECUTE_RESULT_TABLE: ColumnsType<any> = [
  {
    dataIndex: 'id',
    title: 'ID',
    ellipsis: true,
  },
  {
    dataIndex: 'label',
    title: 'Label',
    ellipsis: true,
  },
  {
    dataIndex: 'properties',
    title: 'Properties',
    render: (text) => {
      const JSONtext = JSON.stringify(text);
      return (
        <Tooltip title={JSONtext}>
          <span style={{ cursor: 'pointer' }}>{JSONtext}</span>
        </Tooltip>
      );
    },
    ellipsis: true,
  },
];

export const EXCECUTE_RESULT_TABLE_OPTIONS = [
  { label: '节点数据', value: 'nodes' },
  { label: '边数据', value: 'edges' },
];
export const EXCECUTE_PLAN_RESULT_TABLE: ColumnsType<any> = [
  {
    dataIndex: 'step',
    title: '应用的遍历规则',
  },
  {
    dataIndex: 'type',
    title: '遍历规则分类',
  },
  {
    dataIndex: 'currentSQL',
    title: '遍历器当前状态',
  },
];

export const INIT_GRAPH_MODEL_DATA: GraphinData = {
  nodes: [],
  edges: [],
};

export const TTL_INDEX_OPTIONS = [
  {
    label: 'TTL配置',
    value: 'ttl',
    tip: '字段对应的真实时间+TTL（时间窗口长度）>当前时间，保留该节点，否则删除；目前仅支持一个TTL配置',
  },
  {
    label: '索引配置',
    value: 'index',
    tip: '可针对单个/多个字段组合进行索引配置',
  },
];

export const SCHEMA_KEY_WORDS = ['SRCID', 'ID', 'DSTID', 'TIMESTAMP'];

export const SEGMENTED_OPTIONS = [
  { label: '点', value: 'node' },
  { label: '边', value: 'edge' },
];

export const MODEL_OVER_VIEW_TABS = [
  { key: 'list', text: '列表' },
  { key: 'graph', text: '图谱' },
];
