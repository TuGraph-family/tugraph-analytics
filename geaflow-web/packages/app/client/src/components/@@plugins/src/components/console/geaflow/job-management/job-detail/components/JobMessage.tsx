import { Input, Modal, Button, Space, Tag, Table, Tooltip } from "antd";
import React, { useEffect, useState } from "react";
import { getJobErrorMessage } from "../../../services/job-detail";
import { json } from "@codemirror/lang-json";
import CodeMirror from "@uiw/react-codemirror";
import moment from "moment";
import styles from "../index.module.less";
import $i18n from "../../../../../../../../i18n";

const { Search } = Input;

interface JobMessageProps {
  jobItem: any;
}

export const JobMessage: React.FC<JobMessageProps> = ({ jobItem }) => {
  const [state, setState] = useState({
    current: [],
    origin: [],
  });
  const [currentMessage, setCurrentMessage] = useState({});
  const [visible, setVisible] = useState(false);

  const messageHandle = async () => {
    const resp = await getJobErrorMessage(jobItem.id);

    setState({
      ...state,
      current: resp.list,
      origin: resp.list,
    });
  };

  useEffect(() => {
    messageHandle();
  }, [jobItem.id]);

  const handleShowModal = (record) => {
    setVisible(true);
    setCurrentMessage(record);
  };

  const columns = [
    {
      title: "PID",
      dataIndex: "processId",
      key: "processId",
      width: 100,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobMessage.Time",
        dm: "发生时间",
      }),
      dataIndex: "timeStamp",
      key: "timeStamp",
      width: 250,
      render: (text: string) => {
        if (!text) {
          return "-";
        }
        return moment(Number(text)).format("YYYY-MM-DD HH:mm:ss");
      },
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobMessage.Host",
        dm: "服务器",
      }),
      dataIndex: "hostname",
      key: "hostname",
      width: 500,
      ellipsis: {
        showTitle: false,
      },
      render: (hostname: string) => (
        <Tooltip placement="topLeft" title={hostname}>
          {hostname || "-"}
        </Tooltip>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobMessage.Type",
        dm: "类型",
      }),
      dataIndex: "severity",
      key: "severity",
      width: 100,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.JobMessage.Log",
        dm: "日志",
      }),
      dataIndex: "message",
      key: "message",
      width: 100,
      ellipsis: {
        showTitle: false,
      },
      render: (message: string, record: any) => (
        <a onClick={() => handleShowModal(record)}>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.JobMessage.ViewDetails",
            dm: "查看详情",
          })}
        </a>
      ),
    },
  ];

  return (
    <div className={styles["graph-message"]}>
      <Table
        columns={columns}
        dataSource={state.current}
        pagination={{
          pageSize: 10,
        }}
      />

      <Modal
        title={$i18n.get({
          id: "openpiece-geaflow.job-detail.components.JobMessage.LogDetails",
          dm: "日志详情",
        })}
        width={1200}
        visible={visible}
        footer={[
          <Button onClick={() => setVisible(false)}>
            {$i18n.get({
              id: "openpiece-geaflow.job-detail.components.JobMessage.Close",
              dm: "关闭",
            })}
          </Button>,
        ]}
        onCancel={() => setVisible(false)}
        className={styles["message-model"]}
      >
        <CodeMirror
          value={currentMessage.message}
          extensions={[json()]}
          style={{ height: 500, overflow: "auto" }}
        />
      </Modal>
    </div>
  );
};
