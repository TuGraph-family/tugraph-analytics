import { Button, Tabs, Popconfirm, Form } from "antd";
import React, { useState, useEffect } from "react";
import { PlusOutlined } from "@ant-design/icons";
import { GraphDefintionEditTable } from "./graphDefintionEditTable";
import { DeleteOutlined } from "@ant-design/icons";
import styles from "./index.less";
import { isEmpty, cloneDeep } from "lodash";
import $i18n from "@/components/i18n";

type Props = {
  form: any;
  serveList: any;
  fields: any;
  check: boolean;
  tableList: any;
};

export const GraphDefintionTab: React.FC<Props> = ({
  form,
  serveList,
  tableList,
  fields,
  check,
}) => {
  const [tabsData, setTabsData] = useState([]);

  useEffect(() => {
    if (!isEmpty(fields)) {
      setTabsData(fields);
    }
  }, [fields]);

  //删除事件
  const handleDelete = (paneIndex: number) => (
    <Popconfirm
      title={$i18n.get({
        id: "openpiece-geaflow.geaflow.graph-tabs.add.AreYouSureYouWant",
        dm: "你确定要删除吗?",
      })}
      placement="topRight"
      onConfirm={(event) => {
        event.stopPropagation();
        const deleteData = cloneDeep(tabsData);
        deleteData.splice(paneIndex, 1);
        setTabsData([...deleteData]);
      }}
      okText="确认"
      cancelText="取消"
    >
      {!check ? (
        <DeleteOutlined
          disabled={check}
          onClick={(event) => {
            event.stopPropagation();
          }}
        />
      ) : (
        <div></div>
      )}
    </Popconfirm>
  );

  const addPanel = () => {
    const dataItem = {
      id: Math.random(),
    };
    const addData = cloneDeep(tabsData);
    addData.push(dataItem);
    setTabsData([...addData]);
  };

  return (
    <div>
      {/* {tabsData.length > 0 && !check && (
        <span style={{ fontWeight: "bold", marginLeft: 16 }}>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.TargetStructTable",
            dm: "目标点边只能来自一个输入表, 且输入表字段和目标点边属性的类型需要一致.",
          })}
        </span>
      )} */}
      <div className={styles["graph-tab"]}>
        {tabsData?.map((i, paneIndex) => {
          return (
            <GraphDefintionEditTable
              genExtra={handleDelete}
              paneIndex={paneIndex}
              form={form}
              tabsData={tabsData}
              serveList={serveList}
              tableList={tableList}
              check={check}
              item={i}
              setTabsData={setTabsData}
            />
          );
        })}
        {tabsData.length > 0 && !check && (
          <Button
            className={styles["graph-tab-btn"]}
            disabled={check}
            type="dashed"
            icon={<PlusOutlined />}
            onClick={() => {
              addPanel();
            }}
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.graph-tabs.graphDefintionEditTable.AddStructMapping",
              dm: "添加映射项",
            })}
          </Button>
        )}
      </div>
    </div>
  );
};
