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

import React, { useState } from "react";
import { history, useLocation } from "umi";
import { Menu } from "antd";
import type { MenuProps } from "antd";
import menuOfRoute from "./MenuRoute";
import styles from "./ NavigationMenu.module.less";

type childProps = {
  key: string;
  icon?: React.ReactNode;
  label?: string;
  title?: string;
  children?: childProps[];
};

type MenuStatus = {
  activeKey: string;
  openkeys: string[];
};

interface NavigationMenuProps {
  data: childProps[];
  defaultActive: string;
  defaultSelect: string;
}

const getRouterActivityKey = (
  routes: childProps[],
  currentPathName: string,
  currentKey: string = ""
) => {
  let menuStatus: MenuStatus = { activeKey: "", openkeys: [] };
  const routerMapping = (
    routes: childProps[],
    currentPathName: string,
    currentKey: string = ""
  ) => {
    routes.forEach((a) => {
      if (!a.children) {
        if (
          menuOfRoute
            .filter((m) => m.activeKey === a.key)[0]
            ?.routes.includes(currentPathName)
        ) {
          menuStatus.activeKey = a.key;
          menuStatus.openkeys.push(currentKey);
          return;
        }
      } else {
        routerMapping(a.children, currentPathName, a.key);
      }
    });
  };
  routerMapping(routes, currentPathName, currentKey);
  return menuStatus;
};

export const NavigationMenu: React.FC<NavigationMenuProps> = ({
  data,
  defaultActive,
  defaultSelect,
}) => {
  const location = useLocation();
  const [menuStatus, setMenuStatus] = useState<MenuStatus>(
    getRouterActivityKey(data, location.pathname)
  );

  const handleMenuClick = (item: any) => {
    history.push(item.key);
  };
  const rootSubmenuKeys = data.map((item) => item.key);

  const onOpenChange: MenuProps["onOpenChange"] = (keys) => {
    const latestOpenKey = keys.find(
      (key) => menuStatus.openkeys.indexOf(key) === -1
    );
    if (rootSubmenuKeys.indexOf(latestOpenKey!) === -1) {
      setMenuStatus({
        ...menuStatus,
        openkeys: keys,
      });
    } else {
      setMenuStatus({
        ...menuStatus,
        openkeys: latestOpenKey ? [latestOpenKey] : [],
      });
    }
  };

  return (
    <Menu
      mode="inline"
      className={styles["layout-menu"]}
      items={data}
      onClick={handleMenuClick}
      defaultOpenKeys={[defaultActive]}
      defaultSelectedKeys={[menuStatus.activeKey || defaultSelect]}
      activeKey={menuStatus.activeKey}
      // openKeys={menuStatus.openkeys}
      onOpenChange={onOpenChange}
    />
  );
};
