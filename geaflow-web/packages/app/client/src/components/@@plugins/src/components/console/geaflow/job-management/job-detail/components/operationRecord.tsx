import React, { useEffect, useState, useRef } from "react";
import { Modal, Button, Table, Tooltip } from "antd";
import type { ActionType } from "@ant-design/pro-components";
import { getRecordList } from "../../../services/job-detail";
import moment from "moment";

interface OperationRecordProps {
  jobId: string;
  visible: boolean;
  onClose: () => void;
}

const OperationRecord: React.FC<OperationRecordProps> = ({
  jobId,
  visible,
  onClose,
}) => {
  const [state, setState] = useState({
    recordList: [],
    page: 1,
    total: 0,
  });
  const status = {
    START: "启动",
    STOP: "停止",
    CREATE: "创建",
  };
  const columns = [
    {
      title: "操作人",
      key: "creatorName",
      align: "center",
      render: (_, record: any) => <span>{record.creatorName}</span>,
    },
    {
      title: "操作时间",
      key: "createTime",
      align: "center",
      render: (_, record: any) => <span>{record.createTime}</span>,
    },
    {
      title: "操作",
      dataIndex: "operationType",
      key: "operationType",
      align: "center",
      render: (_, record: any) => (
        <span>{status[record.operationType] || record.operationType}</span>
      ),
    },
    {
      title: "详情信息",
      dataIndex: "detail",
      key: "detail",
      align: "center",
      ellipsis: {
        showTitle: false,
      },
      render: (detail: string) => (
        <Tooltip placement="topLeft" title={detail}>
          {detail || "-"}
        </Tooltip>
      ),
    },
  ];

  const queryRecordList = async (id: string) => {
    const response = await getRecordList(id, state.page);
    if (response) {
      setState({
        ...state,
        total: response.total,
        recordList: response.list,
      });
    }
  };

  useEffect(() => {
    queryRecordList(jobId);
  }, [jobId, state.page]);

  return (
    <Modal
      visible={visible}
      open={visible}
      onCancel={onClose}
      width={835}
      title="操作记录"
      footer={<Button onClick={onClose}>关闭</Button>}
    >
      <Table
        columns={columns}
        dataSource={state.recordList}
        pagination={{
          pageSize: 10,
          total: state.total,
          onChange: (page, pageSize) => {
            setState({ ...state, page: page });
          },
        }}
      />
    </Modal>
  );
};

export default OperationRecord;
