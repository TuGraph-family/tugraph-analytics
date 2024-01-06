import { GraphinContext } from '@antv/graphin';
import { Popover } from 'antd';
import React, { useCallback, useContext } from 'react';
import { useGraphinContext } from '../../../components/garph-canvas';
import IconFont from '../../../components/icon-font/index';

const AutoZoom: React.FC = () => {
  const { apis } = useGraphinContext();
  const { apis: contextApis } = useContext(GraphinContext);

  const onClick = useCallback(() => {
    if (apis) {
      apis.handleAutoZoom();
    }
    if (contextApis.handleAutoZoom) {
      contextApis.handleAutoZoom();
    }
  }, [apis, contextApis]);
  return (
    <Popover content="全屏" placement="top">
      <div onClick={onClick}>
        <IconFont type="icon-compress" />
      </div>
    </Popover>
  );
};

export default AutoZoom;
