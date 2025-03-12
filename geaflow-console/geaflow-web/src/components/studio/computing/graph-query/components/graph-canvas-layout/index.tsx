/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
