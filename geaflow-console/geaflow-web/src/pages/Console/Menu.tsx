import { ControlOutlined } from "@ant-design/icons";
import React from "react";
import $i18n from "@/components/i18n";

export const MENU_List_MAP = [
  {
    key: "/console",
    label: $i18n.get({
      id: "openpiece-geaflow.OPERATION",
      dm: "运维中心",
    }),
    icon: <ControlOutlined />,
    children: [
      {
        key: "/console/ColJobList",
        label: $i18n.get({
          id: "openpiece-geaflow.Tasks",
          dm: "作业管理",
        }),
      },
      {
        key: "/console/ColGeaflowPlugManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Plugins",
          dm: "插件管理",
        }),
      },
      {
        key: "/console/ColInstanceList",
        label: $i18n.get({
          id: "openpiece-geaflow.Instances",
          dm: "实例管理",
        }),
      },
      {
        key: "/console/ColGeaflowJarfileManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Files",
          dm: "文件管理",
        }),
      },
      {
        key: "/console/ColUserManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Users",
          dm: "用户管理",
        }),
      },
    ],
  },
];
