import {
  DeleteOutlined,
  EditOutlined,
  MacCommandOutlined,
  RedoOutlined,
} from "@ant-design/icons";
import { Input, Table, Select, Tooltip, Button, Tag } from "antd";
import { filter, join, map, isEmpty } from "lodash";
import React, { useEffect } from "react";
import { useImmer } from "use-immer";
import SearchInput from "../search-input";
import SwitchDrawer from "../switch-drawer";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import { useVisible } from "../hooks/useVisible";
import { getOlaps } from "../../../../services/computing";
import $i18n from "@/components/i18n";
import styles from "./index.module.less";

const { Option } = Select;
type Prop = {
  onSelect: (id: string, result: string, record: any, script: string) => void;
  instance?: any;
  isOlaps: string;
  record?: any;
};

export const StatementList: React.FC<Prop> = ({
  onSelect,
  instance,
  isOlaps,
  record,
}) => {
  const { visible, onShow, onClose } = useVisible({ defaultVisible: true });
  const [state, updateState] = useImmer<{
    queryList: Array<{
      id: string;
      createTime: string;
      script: string;
      status?: boolean;
      result: string;
    }>;
    activeId: string;
    dataTotal: number;
    page: number;
    pageSize: number;
  }>({
    queryList: [],
    activeId: "",
    dataTotal: 0,
    page: 1,
    pageSize: 10,
  });
  const { queryList, dataTotal, page, pageSize } = state;
  const tagColor = {
    FAILED: "error",
    FINISHED: "success",
    RUNNING: "processing",
  };

  const columns = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.query.Status",
        dm: "状态",
      }),
      dataIndex: "status",
      key: "status",
      width: 100,
      render: (status) => {
        return <Tag color={tagColor[status]}>{status}</Tag>;
      },
    },
    {
      title: (
        <span>
          <span>
            {$i18n.get({
              id: "openpiece-geaflow.job-detail.components.query.Script",
              dm: "查询语句",
            })}
          </span>
          <a
            style={{ float: "right" }}
            onClick={() => {
              handleInstance();
            }}
          >
            <RedoOutlined />
            {$i18n.get({
              id: "openpiece-geaflow.job-detail.components.query.Refresh",
              dm: "刷新",
            })}
          </a>
        </span>
      ),
      dataIndex: "script",
      key: "script",
      width: 180,
      ellipsis: {
        showTitle: false,
      },
      render: (script, record) => (
        <Tooltip placement="topLeft" title={script}>
          <a
            onClick={() => {
              updateState((draft) => {
                draft.activeId = record.id;
                onSelect?.(record.id, record.result, record, record.script);
              });
            }}
          >
            {script}
          </a>
        </Tooltip>
      ),
    },
  ];

  const handleInstance = async () => {
    if (!isEmpty(instance)) {
      getOlaps({ jobId: instance?.id, page, size: pageSize }).then((res) => {
        updateState((draft) => {
          draft.queryList = [...res?.list];
          draft.dataTotal = res?.total;
        });
        if (!isEmpty(record)) {
          const filters = res?.list.filter((item) => item.id === record?.id);
          if (!isEmpty(filters)) {
            if (
              record.status === "RUNNING" &&
              filters[0].status !== "RUNNING"
            ) {
              onSelect?.(
                filters[0].id,
                filters[0].result,
                filters[0],
                filters[0].script
              );
            }
          }
        }
      });
    }
  };

  useEffect(() => {
    handleInstance();
  }, [instance, page, pageSize, isOlaps]);

  useEffect(() => {
    const getStance = window.setInterval(() => {
      handleInstance();
    }, 6000);
    return () => {
      getStance && window.clearInterval(getStance);
    };
  }, [instance, record]);

  return (
    <div
      className={`${styles[`${PUBLIC_PERFIX_CLASS}-statement`]} ${
        !visible ? `${styles[`${PUBLIC_PERFIX_CLASS}-statement-ani`]}` : ""
      }`}
    >
      <SwitchDrawer
        visible={visible}
        onShow={onShow}
        onClose={onClose}
        position="left"
        className={styles[`${PUBLIC_PERFIX_CLASS}-statement-drawer`]}
        width={350}
        backgroundColor="#f6f6f6"
      >
        <div
          className={styles[`${PUBLIC_PERFIX_CLASS}-statement-drawer-content`]}
        >
          {/* <div
            className={
              styles[`${PUBLIC_PERFIX_CLASS}-statement-drawer-content-title`]
            }
          >
            语句查询
          </div> */}
          <div
            className={
              styles[`${PUBLIC_PERFIX_CLASS}-statement-drawer-content-search`]
            }
          >
            <div
              className={
                styles[`${PUBLIC_PERFIX_CLASS}-statement-drawer-content-list`]
              }
            >
              <Table
                dataSource={queryList}
                columns={columns}
                pagination={{
                  defaultPageSize: 10,
                  hideOnSinglePage: true,
                  total: dataTotal,
                  onChange: (page, pageSize) => {
                    updateState((draft) => {
                      draft.page = page;
                      draft.pageSize = pageSize;
                    });
                  },
                }}
              />
            </div>
          </div>
        </div>
      </SwitchDrawer>
    </div>
  );
};
