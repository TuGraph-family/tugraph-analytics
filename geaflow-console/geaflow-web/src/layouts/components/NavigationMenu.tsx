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
