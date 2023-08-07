import React, { useEffect, useState, useRef } from "react";
import { Modal, Button, Table, Tooltip } from "antd";
import { getRecordList } from "../../../services/job-detail";
import $i18n from "../../../../../../../../i18n";

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
    CREATE: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Create",
      dm: "创建",
    }),
    UPDATE: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Update",
      dm: "更新",
    }),
    DELETE: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Delete",
      dm: "删除",
    }),
    PUBLISH: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Publish",
      dm: "发布",
    }),
    START: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Start",
      dm: "启动",
    }),
    STOP: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Stop",
      dm: "停止",
    }),
    REFRESH: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Refresh",
      dm: "刷新",
    }),
    RESET: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.Reset",
      dm: "重置",
    }),
    STARTUP_NOTIFY: $i18n.get({
      id: "openpiece-geaflow.job-detail.components.operationRecord.StartupNotify",
      dm: "启动通知",
    }),
  };
  const columns = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.operationRecord.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      align: "center",
      render: (_, record: any) => <span>{record.creatorName}</span>,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.operationRecord.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      align: "center",
      render: (_, record: any) => <span>{record.createTime}</span>,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.operationRecord.Operation",
        dm: "操作",
      }),
      dataIndex: "operationType",
      key: "operationType",
      align: "center",
      render: (_, record: any) => (
        <span>{status[record.operationType] || record.operationType}</span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.job-detail.components.operationRecord.Details",
        dm: "详情信息",
      }),
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
      title={$i18n.get({
        id: "openpiece-geaflow.job-detail.components.operationRecord.OperationRecord",
        dm: "操作记录",
      })}
      footer={
        <Button onClick={onClose}>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.operationRecord.Close",
            dm: "关闭",
          })}
        </Button>
      }
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
