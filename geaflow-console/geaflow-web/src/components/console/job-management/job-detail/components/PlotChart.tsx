
import React, { useEffect } from "react";
import { Area, Line } from "@antv/g2plot";

interface PlotChartProps {
  xField: string;
  // 图形在 x 方向对应的数据字段名
  yField: string;
  // 图形在 y 方向对应的数据字段名
  seriesField: string;
  // 分组字段
  data: any;
  // 图标数据
}

export const PlotChart: React.FC<PlotChartProps> = ({
  xField,
  yField,
  seriesField,
  data,
}) => {
  useEffect(() => {
    const area = new Area("container", {
      data,
      xField,
      yField,
      seriesField,
    });
    area.render();

    const line = new Line("containerLine", {
      data,
      padding: "auto",
      xField,
      yField,
      xAxis: {
        tickCount: 5,
      },
    });

    line.render();
  }, []);

  return (
    <div
      style={{
        width: "100%",
        display: "flex",
        justifyContent: "space-around",
      }}
    >
      <div id="container"></div>
      <div id="containerLine"></div>
    </div>
  );
};
