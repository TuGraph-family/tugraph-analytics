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
import type { HTMLAttributes, ReactNode } from "react";
import React from "react";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import IconFont from "../icon-font";
import styles from "./index.module.less";

interface IconItemProps extends HTMLAttributes<HTMLDivElement> {
  icon?: ReactNode;
  name?: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  iconProps?: {
    style?: React.CSSProperties;
  };
  direction?: "horizontal";
}

const IconItem: React.FC<IconItemProps> = ({
  icon,
  name,
  onClick,
  disabled,
  iconProps,
  direction,
  ...others
}) => {
  return (
    <div
      {...others}
      className={join(
        [
          disabled
            ? styles[`${PUBLIC_PERFIX_CLASS}-icon-item-disabled`]
            : styles[`${PUBLIC_PERFIX_CLASS}-icon-item`],
          styles[
            `${PUBLIC_PERFIX_CLASS}-icon-item__${direction || "vertical"}`
          ],
          "icon-item",
        ],
        " "
      )}
      onClick={disabled ? undefined : onClick}
    >
      {typeof icon === "string" ? (
        <IconFont {...iconProps} type={icon} />
      ) : (
        icon
      )}
      {name && (
        <div
          className={join(
            [styles[`${PUBLIC_PERFIX_CLASS}-icon-item-name`], "icon-item-name"],
            " "
          )}
        >
          {name}
        </div>
      )}
    </div>
  );
};

export default IconItem;
