import React from 'react';
import { useFieldSchema } from '@formily/react';
import {
  GeneralSchemaDesigner,
  SchemaSettings,
  useCollection,
  useDesignable,
} from '@tugraph/openpiece-client';
import { map, get } from 'lodash';

export const PluginDesigner = () => {
  const { name, title } = useCollection();
  const fieldSchema = useFieldSchema();

  const openpieceRouteList = localStorage.getItem('OPENPIECE_ROUTE_LIST');
  const routeOptions = openpieceRouteList ? JSON.parse(openpieceRouteList) : [];

  const { dn } = useDesignable();

  return (
    <GeneralSchemaDesigner title={title || name}>
      <SchemaSettings.Remove
        removeParentsIfNoChildren
        breakRemoveOn={{
          'x-component': 'Grid',
        }}
      />
      <SchemaSettings.ModalItem
        key="select-url"
        title="路由配置"
        schema={{
          type: 'object',
          title: '',
          properties: {
            routeValue: {
              title: '',
              'x-component': 'Table.Action',
              'x-decorator': 'FormItem',
              'x-component-props': {
                options: routeOptions,
                routeValue: get(
                  fieldSchema,
                  'properties.row1.x-component-props.routeValue',
                  []
                ),
              },
            },
          },
        }}
        onSubmit={({ routeValue }) => {
          const componentSchema = fieldSchema?.properties?.row1;
          const redirectPath = map(routeValue, (item) => {
            return {
              path: `/admin/${item?.value}`,
              pathName: item.label,
            };
          });
          if (componentSchema) {
            componentSchema['x-component-props'] =
              componentSchema['x-component-props'] || {};
            componentSchema['x-component-props']['redirectPath'] = redirectPath;
            componentSchema['x-component-props']['routeValue'] = routeValue;
            dn.emit('patch', {
              schema: {
                'x-uid': componentSchema['x-uid'],
                'x-component-props': componentSchema['x-component-props'],
              },
            });
            dn.refresh();
          }
        }}
      />
    </GeneralSchemaDesigner>
  );
};
