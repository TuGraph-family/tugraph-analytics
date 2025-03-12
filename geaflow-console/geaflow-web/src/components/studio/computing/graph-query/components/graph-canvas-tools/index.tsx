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
