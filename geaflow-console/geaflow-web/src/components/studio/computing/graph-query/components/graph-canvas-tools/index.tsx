import { join } from "lodash";
import React from "react";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import AutoZoom from "./components/auto-zoom";
import RealZoom from "./components/real-zoom";
import ZoomIn from "./components/zoom-in";
import ZoomOut from "./components/zoom-out";

import styles from "./index.module.less";

export const GraphCanvasTools: React.FC = () => {
  return (
    <div
      className={join(
        [
          styles[`${PUBLIC_PERFIX_CLASS}-graph-canvas-tools`],
          `${PUBLIC_PERFIX_CLASS}-graph-canvas-tools`,
        ],
        " "
      )}
    >
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-graph-canvas-tools-item`]}>
        <ZoomIn />
      </div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-graph-canvas-tools-item`]}>
        <ZoomOut />
      </div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-divider`]} />
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-graph-canvas-tools-item`]}>
        <RealZoom />
      </div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-graph-canvas-tools-item`]}>
        <AutoZoom />
      </div>
    </div>
  );
};
