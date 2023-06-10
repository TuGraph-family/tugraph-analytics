import React, { useContext } from 'react';
import { TableOutlined } from '@ant-design/icons';
import {
  SchemaComponentOptions,
  SchemaInitializer,
  SchemaInitializerContext,
} from '@tugraph/openpiece-client';
// @ts-ignore
import { TableDefinition } from '@@plugins/src/components/studio/geaflow';
import { PluginDesigner } from './PluginDesigner';

export const PluginBlockInitializer = (props) => {
  const { insert } = props;

  const schema = {
    type: 'void',
    'x-component': 'CardItem',
    'x-designer': 'PluginDesigner',
    properties: {
      row1: {
        type: 'void',
        'x-component': 'TableDefinition',
        'x-async': false,
        'x-index': 1,
        'x-component-props': {},
      },
    },
  };

  return (
    <SchemaInitializer.Item
      {...props}
      icon={<TableOutlined />}
      onClick={() => {
        insert(schema);
      }}
      title="TableDefinition"
    />
  );
};

export default React.memo((props) => {
  const items = useContext(SchemaInitializerContext) as any;
  const children = items?.BlockInitializers?.items?.[1]?.children ?? [];

  const hasCustomBlock = children?.find(
    (d) => d.key === ' TableDefinition'
  );

  if (!hasCustomBlock) {
    children.push({
      key: ' TableDefinition',
      type: 'item',
      title: ' TableDefinition',
      component: PluginBlockInitializer,
    });
  }
  return (
    <SchemaComponentOptions
      components={{
        PluginDesigner,
        PluginBlockInitializer,
        TableDefinition,
      }}
    >
      <SchemaInitializerContext.Provider value={items}>
        {props.children}
      </SchemaInitializerContext.Provider>
    </SchemaComponentOptions>
  );
});
