import { Button, Tabs, Popconfirm } from "antd";
import React, { useState, useEffect } from "react";
import { PlusOutlined } from "@ant-design/icons";
import { GraphDefintionEditTable } from "./graphDefintionEditTable";
import { DeleteOutlined } from "@ant-design/icons";
import { isEmpty } from "lodash";
import styles from "./index.less";
import $i18n from "@/components/i18n";

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
};

const { TabPane } = Tabs;
export const GraphDefintionTab: React.FC<Props> = ({
  tabsList = [],
  form,
  currentItem,
  readonly,
  editable,
}) => {
  const [tabsData, setTabsData] = useState(tabsList);

  useEffect(() => {
    if (!isEmpty(currentItem)) {
      let TableData = tabsData;
      TableData[0].editTables = [currentItem];
      setTabsData([...TableData]);
    }
  }, [currentItem]);

  return (
    <Tabs className={styles["graph-tab"]}>
      {tabsData.map((item, index) => (
        <TabPane tab={item.name} key={item.type}>
          {item.editTables?.map((i, paneIndex) => {
            return (
              <GraphDefintionEditTable
                type={item.type}
                length={0}
                id={i?.id}
                fields={i?.fields}
                index={index}
                paneIndex={paneIndex}
                form={form}
                name={i?.name}
                readonly={readonly}
                editable={editable}
              />
            );
          })}
        </TabPane>
      ))}
    </Tabs>
  );
};
