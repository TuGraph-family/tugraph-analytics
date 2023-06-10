import React, { useContext } from 'react';
import { TableOutlined } from '@ant-design/icons';
import {
  SchemaComponentOptions,
  SchemaInitializer,
  SchemaInitializerContext,
} from '@tugraph/openpiece-client';
// @ts-ignore
import { GraphDefinition } from '@@plugins/src/components/studio/geaflow';
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
        'x-component': 'GraphDefinition',
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
      title="GraphDefinition"
    />
  );
};

export default React.memo((props) => {
  const items = useContext(SchemaInitializerContext) as any;
  const children = items?.BlockInitializers?.items?.[1]?.children ?? [];

  const hasCustomBlock = children?.find(
    (d) => d.key === ' GraphDefinition'
  );

  if (!hasCustomBlock) {
    children.push({
      key: ' GraphDefinition',
      type: 'item',
      title: ' GraphDefinition',
      component: PluginBlockInitializer,
    });
  }
  return (
    <SchemaComponentOptions
      components={{
        PluginDesigner,
        PluginBlockInitializer,
        GraphDefinition,
      }}
    >
      <SchemaInitializerContext.Provider value={items}>
        {props.children}
      </SchemaInitializerContext.Provider>
    </SchemaComponentOptions>
  );
});
