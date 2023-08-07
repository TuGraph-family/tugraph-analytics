import { Input, Modal, Button, Table, Tag } from "antd";
import React, { useEffect, useState } from "react";
import { getContainer } from "../../../services/job-detail";
import { json } from "@codemirror/lang-json";
import CodeMirror from "@uiw/react-codemirror";
import moment from "moment";
import styles from "../index.module.less";
import $i18n from "../../../../../../../../i18n";

const { Search } = Input;

interface JobContainerProps {
  jobItem: any;
}

export const JobContainer: React.FC<JobContainerProps> = ({ jobItem }) => {
  const [state, setState] = useState({
    current: [],
    origin: [],
    activeNum: 0,
    totalNum: 0,
  });
  const [currentMessage, setCurrentMessage] = useState({});
  const [visible, setVisible] = useState(false);

  const messageHandle = async () => {
    const resp = await getContainer(jobItem.id);

    setState({
      totalNum: resp?.data?.totalNum || 0,
      activeNum: resp?.data?.activeNum || 0,
      current: resp.data?.containers,
      origin: resp.data?.containers,
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
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.container.ReportingTime",
        dm: "上报时间",
      }),
      key: "reportTime",
      width: 200,
      render: (_, record) => {
        return (
          <span>
            {moment(record.lastTimestamp).format("YYYY-MM-DD HH:mm:ss")}
          </span>
        );
      },
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.container.Name",
        dm: "名称",
      }),
      dataIndex: "name",
      key: "name",
      width: 100,
    },
    {
      title: "Host",
      dataIndex: "host",
      key: "host",
      width: 150,
      render: (_, record) => {
        return (
          <span
            style={{
              display: "flex",
              justifyContent: "flex-start",
              alignItems: "center",
            }}
          >
            <div
              style={{
                width: 8,
                height: 8,
                borderRadius: 4,
                backgroundColor: record?.active ? "#52c41a" : "red",
              }}
            ></div>
            <span style={{ paddingLeft: 8 }}>{record?.host}</span>
          </span>
        );
      },
    },
    {
      title: "PID",
      dataIndex: "pid",
      key: "pid",
      width: 100,
    },
    {
      title: "Heap Usage(%)",
      dataIndex: "heapUsedRatio",
      key: "heapUsedRatio",
      width: 100,
      render: (_, record) => {
        return <span>{record?.metrics?.heapUsedRatio}</span>;
      },
    },
    {
      title: "FGC",
      dataIndex: "fgcCount",
      key: "fgcCount",
      width: 100,
      render: (_, record) => {
        return <span>{record?.metrics?.fgcCount}</span>;
      },
    },
    {
      title: "FGCT(s)",
      dataIndex: "fgcTime",
      key: "fgcTime",
      width: 100,
      render: (_, record) => {
        return <span>{record?.metrics?.fgcTime}</span>;
      },
    },
    {
      title: "GCC",
      dataIndex: "gcCount",
      key: "gcCount",
      width: 100,
      render: (_, record) => {
        return <span>{record?.metrics?.gcCount}</span>;
      },
    },
    {
      title: "CPU(%)",
      dataIndex: "processCpu",
      key: "processCpu",
      width: 100,
      render: (_, record) => {
        return <span>{record?.metrics?.processCpu}</span>;
      },
    },
    {
      title: "Load(60s)",
      dataIndex: "avgLoad",
      key: "avgLoad",
      width: 100,
      render: (_, record) => {
        return <span>{record?.metrics?.avgLoad}</span>;
      },
    },
  ];

  return (
    <div className={styles["job-message"]}>
      <div className={styles["message-header"]}>
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.container.ContainerHealthIndicators",
            dm: "Container健康指标：",
          })}
        </span>
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.container.Running",
            dm: "运行中",
          })}

          <Tag color="success" style={{ marginLeft: 8 }}>
            {state.activeNum}
          </Tag>
        </span>
        <span style={{ marginLeft: 32 }}>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.container.AbnormalStatus",
            dm: "状态异常",
          })}

          <Tag color="error" style={{ marginLeft: 8 }}>
            {state.totalNum - state.activeNum}
          </Tag>
        </span>
      </div>
      <div className={styles["message-middle"]}>
        <Table
          columns={columns}
          dataSource={state.current}
          pagination={{
            pageSize: 10,
          }}
        />

        <Modal
          title={$i18n.get({
            id: "openpiece-geaflow.job-detail.components.container.LogDetails",
            dm: "日志详情",
          })}
          width={800}
          visible={visible}
          footer={[
            <Button onClick={() => setVisible(false)}>
              {$i18n.get({
                id: "openpiece-geaflow.job-detail.components.container.Close",
                dm: "关闭",
              })}
            </Button>,
          ]}
          onCancel={() => setVisible(false)}
        >
          <CodeMirror value={currentMessage.message} extensions={[json()]} />
        </Modal>
      </div>
    </div>
  );
};
