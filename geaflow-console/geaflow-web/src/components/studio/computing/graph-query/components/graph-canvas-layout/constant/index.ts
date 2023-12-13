import type { Layout } from "@antv/graphin";

export const CANVAS_LAYOUT: {
  layout: Layout;
  title: string;
  icon: string;
}[] = [
  {
    layout: {
      type: "graphin-force",
      animation: false,
    },
    title: "力导布局",
    icon: "icon-yuanxingbuju",
  },
  {
    layout: {
      type: "concentric",
      preventOverlap: true,
      nodeSize: 150,
    },
    title: "同心圆布局",
    icon: "icon-tongxinyuanbuju",
  },
  {
    layout: {
      type: "grid",
    },
    title: "栅格布局",
    icon: "icon-jingdianlidaoxiangbuju",
  },
];
