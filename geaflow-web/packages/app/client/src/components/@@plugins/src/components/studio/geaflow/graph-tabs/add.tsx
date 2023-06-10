import React, { useState, useEffect } from "react";
import { Collapse, Button, Popconfirm } from "antd";
import { DeleteOutlined } from "@ant-design/icons";

const { Panel } = Collapse;

type colChild = {
  exportName?: string;
  exportCode?: string;
  configList?: any;
};

interface ColProps {
  colData: colChild[];
  placeholder: string;
}

export const AddCollapse: React.FC<ColProps> = ({ colData, placeholder }) => {
  const [colList, setColList] = useState<colChild[]>(colData);
  // 新增
  const addPanel = (index: number) => {
    const { configList } = colList[index];
    const addData = colList;
    const dataItem = {
      name: `任务${configList.length + 1}`,
    };
    addData[index].configList.push(dataItem);
    setColList([...addData]);
  };

  // 删除
  const genExtra = (index: number, configIndex: Number) => (
    <Popconfirm
      title="你确定要删除吗?"
      placement="topRight"
      onConfirm={(event) => {
        event.stopPropagation();
        const deleteData = [...colList];
        deleteData[index].configList.splice(configIndex, 1);
        deleteData[index].configList.forEach((item, i) => {
          item.name = `任务${i + 1}`;
        });
        setColList([...deleteData]);
      }}
      okText="确认"
      cancelText="取消"
    >
      <DeleteOutlined
        onClick={(event) => {
          event.stopPropagation();
          console.log(event, "event");
        }}
      />
    </Popconfirm>
  );

  return (
    <div>
      {colList?.map((item, index) => {
        return (
          <div>
            <Collapse defaultActiveKey={item.configList[0]?.code}>
              {item.configList.map((e: any, configIndex: number) => {
                return (
                  <Panel
                    header={`任务${configIndex + 1}`}
                    key={e.code}
                    extra={genExtra(index, configIndex)}
                  ></Panel>
                );
              })}
            </Collapse>
            <Button
              type="dashed"
              style={{ width: "100%", marginTop: 16 }}
              onClick={() => addPanel(index)}
            >
              + {placeholder}
            </Button>
          </div>
        );
      })}
    </div>
  );
};
