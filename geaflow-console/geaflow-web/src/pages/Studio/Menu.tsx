import { ControlOutlined } from "@ant-design/icons";
import React from "react";
import $i18n from "@/components/i18n";

export const MENU_List_MAP = [
  {
    key: "/studio",
    label: $i18n.get({
      id: "openpiece-geaflow.DEVELOPMENT",
      dm: "图研发",
    }),
    icon: <ControlOutlined />,
    children: [
      {
        key: "/studio/StudioTableDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Tables",
          dm: "表定义",
        }),
      },
      {
        key: "/studio/StudioNodeDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Vertices",
          dm: "点定义",
        }),
      },
      {
        key: "/studio/StudioEdgeDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Edges",
          dm: "边定义",
        }),
      },
      {
        key: "/studio/StudioGraphDefinition",
        label: $i18n.get({
          id: "openpiece-geaflow.Graphs",
          dm: "图定义",
        }),
      },
      {
        key: "/studio/StudioGeaflowFunctionManage",
        label: $i18n.get({
          id: "openpiece-geaflow.Functions",
          dm: "函数定义",
        }),
      },
      {
        key: "/studio/StudioComputing",
        label: $i18n.get({
          id: "openpiece-geaflow.Jobs",
          dm: "图任务",
        }),
      },
    ],
  },
];
