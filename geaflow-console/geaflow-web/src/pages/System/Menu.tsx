import { ControlOutlined } from "@ant-design/icons";
import React from "react";
import $i18n from "@/components/i18n";

export const MENU_List_MAP = [
  {
    key: "/system",
    label: $i18n.get({
      id: "openpiece-geaflow.SYSTEM",
      dm: "系统管理",
    }),
    icon: <ControlOutlined />,
    children: [
      {
        key: "/system/ColClusterManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Clusters",
          dm: "集群管理",
        }),
      },
      {
        key: "/system/ColGeaflowPlugManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Plugins",
          dm: "插件管理",
        }),
      },
      {
        key: "/system/ColLanguageManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Languages",
          dm: "模型管理",
        }),
      },
      {
        key: "/system/ColGeaflowJarfileManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Files",
          dm: "文件管理",
        }),
      },
      {
        key: "/system/ColVersionManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Versions",
          dm: "版本管理",
        }),
      },
      {
        key: "/system/ColUserManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Users",
          dm: "用户管理",
        }),
      },
      {
        key: "/system/ColTenantManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Tenants",
          dm: "租户管理",
        }),
      },
    ],
  },
];
