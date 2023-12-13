import { GraphinContext } from '@antv/graphin';
import { Popover } from 'antd';
import React, { useCallback, useContext } from 'react';
import { useGraphinContext } from '../../../components/garph-canvas';
import IconFont from '../../../components/icon-font/index';

const ZoomIn: React.FC = () => {
  const { apis } = useGraphinContext();
  const { apis: contextApis } = useContext(GraphinContext);

  const onClick = useCallback(() => {
    if (apis) {
      apis.handleZoomIn();
    }
    if (contextApis.handleZoomIn) {
      contextApis.handleZoomIn();
    }
  }, [apis, contextApis]);
  return (
    <Popover content="缩小" placement="top">
      <div onClick={onClick}>
        <IconFont type="icon-huabu-suoxiao" />
      </div>
    </Popover>
  );
};

export default ZoomIn;
