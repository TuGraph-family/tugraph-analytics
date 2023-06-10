/* eslint-disable no-param-reassign */
/* eslint-disable @typescript-eslint/no-unused-vars */
import React, { useEffect, useState } from "react";
import { DownOutlined } from "@ant-design/icons";
import { Button, Dropdown, Space, Menu, Tag } from "antd";
import cx from "classnames";
import { useOpenpieceUserAuth } from "@tugraph/openpiece-client";
import cls from "./index.less";
import { useAuth } from "../hooks/useAuth";
import { queryInstanceList } from "../services/instance";
import { switchUserRole } from "../services/quickInstall";

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaflowHeader: React.FC<PluginPorps> = ({ redirectPath }) => {
  const { redirectLoginURL, switchRole } = useOpenpieceUserAuth();
  const redirectUrl = "/";

  const isAdminLogin = localStorage.getItem("IS_GEAFLOW_ADMIN");
  const [state, setState] = useState({
    instanceList: [],
    isAdminLogin,
    currentInstance: isAdminLogin
      ? []
      : localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
      ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
      : null,
  });

  const { onLogout } = useAuth();

  const handleLogout = () => {
    onLogout().then((res) => {
      if (res.code === "SUCCESS") {
        localStorage.removeItem("GEAFLOW_LOGIN_USERNAME");
        localStorage.removeItem("QUICK_INSTALL_PARAMS");
        localStorage.removeItem("GEAFLOW_CURRENT_INSTANCE");
        localStorage.removeItem("GEAFLOW_TOKEN");
        localStorage.removeItem("IS_GEAFLOW_ADMIN");
        localStorage.removeItem("HAS_EXEC_QUICK_INSTALL");
        window.location.href = redirectUrl;
      } else {
        if (res.code === "FORBIDDEN") {
          redirectLoginURL();
          return;
        }
      }
    });
  };

  const getInstanceList = async () => {
    const resp = await queryInstanceList();
    // 如果没有登录或没有权限，直接跳转到登录页面
    if (!resp || resp.code === "FORBIDDEN") {
      redirectLoginURL();
      return;
    }

    if (resp.code === "SUCCESS") {
      // 是否存在默认的 Instance
      const defaultSelectInstance = localStorage.getItem(
        "GEAFLOW_CURRENT_INSTANCE"
      );
      if (!defaultSelectInstance) {
        console.log(defaultSelectInstance);
        const defaultInstance = resp.data?.list[0];
        if (defaultInstance) {
          localStorage.setItem(
            "GEAFLOW_CURRENT_INSTANCE",
            JSON.stringify({
              key: defaultInstance.id,
              value: defaultInstance.name,
            })
          );
          setState({
            ...state,
            instanceList: resp.data?.list,
            currentInstance: {
              key: defaultInstance.id,
              value: defaultInstance.name,
            },
          });
        }
      } else {
        setState({
          ...state,
          instanceList: resp.data?.list,
          currentInstance: JSON.parse(defaultSelectInstance),
        });
      }
    }
  };

  useEffect(() => {
    // 管理员登录时候不获取实例列表
    if (!state.isAdminLogin) {
      getInstanceList();
    }
  }, [state.isAdminLogin]);

  const handleSwitchRole = async () => {
    const resp = await switchUserRole();
    // 切换角色成功后，修改 isAdminLogin 的值，并且重新加载页面
    if (resp.code === "SUCCESS") {
      // 清除之前缓存的实例
      localStorage.removeItem("GEAFLOW_CURRENT_INSTANCE");
      localStorage.removeItem("QUICK_INSTALL_PARAMS");

      const adminStatus = localStorage.getItem("IS_GEAFLOW_ADMIN");
      if (adminStatus) {
        // 已经是管理员，则需要切换成非管理员
        localStorage.removeItem("IS_GEAFLOW_ADMIN");

        // Openpiece 角色切换为 member
        switchRole("member", redirectUrl);

        setState({
          ...state,
          isAdminLogin: null,
        });
      } else {
        localStorage.setItem("IS_GEAFLOW_ADMIN", "true");
        const clusterURL = redirectPath?.find(d => d.pathName === '集群管理')
        
        // Openpiece 角色切换为 admin
        switchRole("admin", clusterURL?.path || redirectUrl);
        setState({
          ...state,
          isAdminLogin: "true",
        });
      }
    }
  };

  const items = (
    <Menu>
      {state.isAdminLogin ? (
        <Menu.Item onClick={handleSwitchRole}>进入租户模式</Menu.Item>
      ) : (
        <Menu.Item onClick={handleSwitchRole}>进入系统模式</Menu.Item>
      )}
      <Menu.Item onClick={handleLogout}>注销</Menu.Item>
    </Menu>
  );

  const onChangeInstance = (value) => {
    console.log(value);
    const { key } = value;
    const [k, v] = key.split(",");
    const current = {
      key: k,
      value: v,
    };
    setState({
      ...state,
      currentInstance: current,
    });

    localStorage.setItem("GEAFLOW_CURRENT_INSTANCE", JSON.stringify(current));
    // 切换实例以后，重新加载页面
    window.location.reload();
  };

  const instanceItems = (
    <Menu onClick={onChangeInstance}>
      {state.instanceList.map((item) => {
        return (
          <Menu.Item key={`${item.id},${item.name}`}>{item.name}</Menu.Item>
        );
      })}
    </Menu>
  );

  return (
    <div className={cx(cls["gm-header"])}>
      <div className={cls.right}>
        <div className="gm-header-toolbar">
          {!state.isAdminLogin && (
            <>
              {state.instanceList.length === 0 ? (
                <Button type="text" onClick={(e) => e.preventDefault()}>
                  <Space>
                    <Tag color="red">请先创建实例</Tag>
                  </Space>
                </Button>
              ) : (
                <Dropdown overlay={instanceItems}>
                  <Button type="text" onClick={(e) => e.preventDefault()}>
                    <Space>
                      {state.currentInstance?.value || "请选择实例"}
                      <DownOutlined />
                    </Space>
                  </Button>
                </Dropdown>
              )}
            </>
          )}

          <Dropdown overlay={items}>
            <Button type="text" onClick={(e) => e.preventDefault()}>
              <Space>
                欢迎你，{localStorage.getItem("GEAFLOW_LOGIN_USERNAME")}
                <DownOutlined />
              </Space>
            </Button>
          </Dropdown>
        </div>
      </div>
    </div>
  );
};
