import { Button, Tabs, Popconfirm } from "antd";
import React, { useState, useEffect } from "react";
import { PlusOutlined } from "@ant-design/icons";
import { GraphDefintionEditTable } from "./graphDefintionEditTable";
import { DeleteOutlined } from "@ant-design/icons";
import { GraphDefinitionConfigPanel } from "../graph-tabs/graphDefinitionConfigPanel";
import { isEmpty } from "lodash";
import styles from "./index.less";
import $i18n from "../../../../../../i18n";

type Props = {
  tabsList: {
    name: string;
    type: string;
    editTables: any;
  }[];
  form: any;
  currentItem?: any;
  readonly: boolean;
  editable?: boolean;
  activeKey?: string;
  setActiveKey?: any;
};

const TYPE_TEXT = {
  VERTEX: $i18n.get({
    id: "openpiece-geaflow.geaflow.graph-tabs.AddAPoint",
    dm: "添加一个点",
  }),
  EDGE: $i18n.get({
    id: "openpiece-geaflow.geaflow.graph-tabs.AddAnEdge",
    dm: "添加一条边",
  }),
  TABLE: $i18n.get({
    id: "openpiece-geaflow.geaflow.graph-tabs.AddAnInputTable",
    dm: "添加一个输入表",
  }),
};

const { TabPane } = Tabs;
export const GraphDefintionTab: React.FC<Props> = ({
  tabsList = [],
  form,
  currentItem,
  readonly,
  editable,
  activeKey,
  setActiveKey,
}) => {
  const [tabsData, setTabsData] = useState(tabsList);

  const allType = {
    VERTEX: "pointName",
    EDGE: "sideName",
    TABLE: "tableName",
  };
  const handleFileds = (value: any, type: string, index: number) => {
    return value?.map((item: any) => {
      return {
        [allType[type] + index]: item.name,
        type: item.type,
        comment: item.comment,
        category: item.category,
        id: item.id,
        tenantId: item.tenantId,
      };
    });
  };

  useEffect(() => {
    if (!isEmpty(currentItem)) {
      if (currentItem.type !== "TABLE") {
        let copyData = tabsData;
        copyData[0].editTables = currentItem?.vertices;
        copyData[1].editTables = currentItem?.edges;
        setTabsData([...copyData]);
      } else {
        let TableData = tabsData;
        TableData[0].editTables = [currentItem];
        setTabsData([...TableData]);
      }
    }
  }, [currentItem]);
  //删除事件
  const handleDelete = (index: number, paneIndex: number) => (
    <Popconfirm
      title={$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.AreYouSureYouWant",
        dm: "你确定要删除吗?",
      })}
      placement="topRight"
      onConfirm={(event) => {
        event.stopPropagation();
        const deleteData = [...tabsData];
        deleteData[index].editTables.splice(paneIndex, 1);
        setTabsData([...deleteData]);
      }}
      okText={$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.Confirm",
        dm: "确认",
      })}
      cancelText={$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.Cancel",
        dm: "取消",
      })}
    >
      <DeleteOutlined
        onClick={(event) => {
          event.stopPropagation();
        }}
      />
    </Popconfirm>
  );

  const addPanel = (index: number) => {
    const { type } = tabsData[index];
    const dataItem = {
      id: index,
      type,
      fields: [],
    };
    const addData = tabsData;
    addData[index].editTables.push(dataItem);
    setTabsData([...addData]);
  };

  return (
    <Tabs
      className={styles["graph-tab"]}
      activeKey={activeKey}
      onChange={(key) => {
        setActiveKey(key);
      }}
    >
      {tabsData.map((item, index) => (
        <TabPane tab={item.name} key={item.type}>
          {["tableConfig", "paramConfig"].includes(item.type) ? (
            <GraphDefinitionConfigPanel
              prefixName={
                item.type === "paramConfig" ? "pluginConfig" : "tableConfig"
              }
              form={form}
              readonly={!editable && readonly}
            />
          ) : (
            item.editTables?.map((i, paneIndex) => {
              return (
                <GraphDefintionEditTable
                  type={item.type}
                  length={0}
                  genExtra={handleDelete}
                  id={i?.id}
                  fields={handleFileds(i?.fields, i.type, paneIndex)}
                  index={index}
                  paneIndex={paneIndex}
                  form={form}
                  name={i?.name}
                  readonly={readonly}
                  editable={editable}
                />
              );
            })
          )}

          {(!readonly || editable) &&
            !["tableConfig", "paramConfig", "TABLE"].includes(item.type) && (
              <Button
                className={styles["graph-tab-btn"]}
                type="dashed"
                icon={<PlusOutlined />}
                onClick={() => {
                  addPanel(index);
                }}
              >
                {TYPE_TEXT[item.type]}
              </Button>
            )}
        </TabPane>
      ))}
    </Tabs>
  );
};
