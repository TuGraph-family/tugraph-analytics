/* eslint-disable no-param-reassign */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { Link, history, useLocation } from "umi";
import React, { useEffect, useMemo } from "react";
import { Menu } from "antd";
import cx from "classnames";
import { useImmer } from "use-immer";
import cls from "./index.less";
import { GeaflowHeader } from "@/components/console/header";
import $i18n from "@/components/i18n";

const airportOptions = [
  {
    name: $i18n.get({
      id: "openpiece-geaflow.DEVELOPMENT",
      dm: "图研发",
    }),
    path: "/studio",
  },
  {
    name: $i18n.get({
      id: "openpiece-geaflow.OPERATION",
      dm: "运维中心",
    }),
    path: "/console",
  },
];
const systemOptions = [
  {
    name: $i18n.get({
      id: "openpiece-geaflow.INSTALLATION",
      dm: "一键安装",
    }),
    path: "/quickInstall",
  },
  {
    name: $i18n.get({
      id: "openpiece-geaflow.SYSTEM",
      dm: "系统管理",
    }),
    path: "/system",
  },
];

const getMatchPath = (pathname: string) => {
  if (pathname === "/") {
    return { path: "/" };
  }
  return (
    [...airportOptions, ...systemOptions].find((c) => {
      return c.path.includes(pathname) || pathname.includes(c.path);
    }) || { path: "/" }
  );
};

const Header = ({ isStick }: { isStick?: boolean }) => {
  const location = useLocation();
  const match = useMemo(
    () => getMatchPath(location.pathname),
    [location.pathname]
  );

  const [state, setState] = useImmer<{
    selectedKeys: string[];
  }>({
    selectedKeys: match?.path ? [match?.path] : ["/"],
  });
  const { selectedKeys } = state;
  const onMenuSelect = (e: { key: string }) => {
    setState((draft) => {
      draft.selectedKeys = [e.key];
    });
  };

  useEffect(() => {
    if (!match) {
      return;
    }
    setState((draft) => {
      draft.selectedKeys = [match.path];
    });
  }, [match]);
  const isAdminLogin = localStorage.getItem("IS_GEAFLOW_ADMIN");

  const options = isAdminLogin ? systemOptions : airportOptions;

  return (
    <div className={cx(cls["gm-header"])}>
      <div className={cls.left}>
        <img
          src="https://mdn.alipayobjects.com/huamei_qcdryc/afts/img/A*AbamQ5lxv0IAAAAAAAAAAAAADgOBAQ/original"
          alt=""
          className="logo"
          style={{ cursor: "pointer", padding: "0 24px" }}
        />
        <div style={{ width: "100%" }}>
          <Menu
            selectedKeys={selectedKeys}
            onSelect={onMenuSelect}
            mode="horizontal"
          >
            {options?.map((c) => {
              const { path, name, children } = c;
              if (children) {
                return (
                  <Menu.SubMenu
                    key={path}
                    style={{ padding: "0 12px" }}
                    title={name}
                  >
                    {children.map((item) => {
                      return (
                        <Menu.Item key={item.path}>
                          <Link to={item?.path}>{item?.name}</Link>
                        </Menu.Item>
                      );
                    })}
                  </Menu.SubMenu>
                );
              }
              return (
                <Menu.Item key={path} style={{ padding: "0 12px" }}>
                  <Link to={path}> {name}</Link>
                </Menu.Item>
              );
            })}
          </Menu>
        </div>
      </div>
      <div className={cls.right}>
        <GeaflowHeader />
      </div>
    </div>
  );
};

export default Header;
