import type { Layout } from "@antv/graphin";
import { Popover } from "antd";
import { join } from "lodash";
import React from "react";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import IconFont from "../icon-font";
import { CANVAS_LAYOUT } from "./constant";

import styles from "./index.module.less";

interface GraphCanvasLayoutProps {
  onLayoutChange: (layout: Layout) => void;
  currentLayout: Layout;
}

export const GraphCanvasLayout: React.FC<GraphCanvasLayoutProps> = ({
  onLayoutChange,
  currentLayout,
}) => {
  return (
    <div
      className={join(
        [
          styles[`${PUBLIC_PERFIX_CLASS}-graph-canvas-layout`],
          "graph-canvas-layout",
        ],
        " "
      )}
    >
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-types`]}>
        {CANVAS_LAYOUT.map((item) => {
          const { icon, title, layout } = item;
          return (
            <Popover content={title} placement="top" key={title}>
              <div
                className={
                  currentLayout.type === layout.type
                    ? join(
                        [
                          styles[`${PUBLIC_PERFIX_CLASS}-types-item`],
                          styles[`${PUBLIC_PERFIX_CLASS}-types-item-active`],
                        ],
                        " "
                      )
                    : styles[`${PUBLIC_PERFIX_CLASS}-types-item`]
                }
              >
                <IconFont type={icon} onClick={() => onLayoutChange(layout)} />
              </div>
            </Popover>
          );
        })}
      </div>
    </div>
  );
};
