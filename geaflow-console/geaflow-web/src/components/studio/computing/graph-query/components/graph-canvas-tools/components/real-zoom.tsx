import { GraphinContext } from '@antv/graphin';
import { Popover } from 'antd';
import React, { useCallback, useContext } from 'react';
import { useGraphinContext } from '../../../components/garph-canvas';
import IconFont from '../../../components/icon-font/index';

const RealZoom: React.FC = () => {
  const { apis } = useGraphinContext();
  const { apis: contextApis } = useContext(GraphinContext);

  const onClick = useCallback(() => {
    if (apis) {
      apis.handleRealZoom();
    }
    if (contextApis.handleRealZoom) {
      contextApis.handleRealZoom();
    }
  }, [apis, contextApis]);
  return (
    <Popover content="画布1:1" placement="top">
      <div onClick={onClick}>
        <IconFont type="icon-icon-test" />
      </div>
    </Popover>
  );
};

export default RealZoom;
