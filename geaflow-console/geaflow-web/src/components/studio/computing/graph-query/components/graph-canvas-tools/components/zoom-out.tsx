import { GraphinContext } from '@antv/graphin';
import { Popover } from 'antd';
import React, { useCallback, useContext } from 'react';
import { useGraphinContext } from '../../../components/garph-canvas';
import IconFont from '../../../components/icon-font/index';
const ZoomOut: React.FC = () => {
  const { apis } = useGraphinContext();
  const { apis: contextApis } = useContext(GraphinContext);
  const onClick = useCallback(() => {
    if (apis) {
      apis.handleZoomOut();
    }
    if (contextApis.handleZoomOut) {
      contextApis.handleZoomOut();
    }
  }, [apis, contextApis]);
  return (
    <Popover content="放大" placement="top">
      <div onClick={onClick}>
        <IconFont type="icon-huabu-fangda" />
      </div>
    </Popover>
  );
};

export default ZoomOut;
