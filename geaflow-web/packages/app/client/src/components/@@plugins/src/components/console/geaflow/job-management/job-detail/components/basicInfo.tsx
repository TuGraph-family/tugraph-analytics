import React, { useState, useEffect } from "react";
import {
  Card,
  Button,
  Dropdown,
  Row,
  Col,
  Space,
  message,
  Tag,
  Modal,
  Form,
  Select,
  Menu,
} from "antd";
import type { MenuProps } from "antd";
import { ReloadOutlined, ExclamationCircleOutlined } from "@ant-design/icons";
import {
  resetJob,
  getOperations,
  getTaskIdStatus,
  getReleases,
  getApiVersions,
} from "../../../services/job-detail";
import BasicTabs from "./basicTabs";
import OperationRecord from "./operationRecord";
import { isEmpty } from "lodash";

interface BasicInfoProps {
  currentStage: string;
  jobItem: any;
  onCreate?: () => void;
  onDelete?: () => void;
  uniqueId: string;
}
interface SLALevelResponse {
  alertEnable: boolean;
  creator: string;
  domainCode: string;
  gmtCreate: number;
  gmtModified: number;
  id: string;
  instanceId: string;
  jobUniqueId: number;
  level: string;
  modifier: string;
}
const STATUS_COLOR_MAPPING = {
  CREATED: "default",
  STOPPED: "red",
  RUNNING: "green",
  WAITING: "green",
  STARTING: "processing",
  FAILED: "error",
  FINISHED: "green",
  DELETED: "red",
};
const BasicInfo: React.FC<BasicInfoProps> = ({ jobItem, uniqueId }) => {
  const [form] = Form.useForm();
  const [state, setState] = useState<{
    detailInfo: any;
    slaInfo: SLALevelResponse | null;
    engineOptions: {
      label: string;
      value: string;
    }[];
    currentVersion: string;
    clusterConfig: string;
    clusterName: string;
    mainClass: string;
    taskConfig: string;
    currentStatus: string;
    dslConfig: string;
    jarUrl: any;
  }>({
    detailInfo: null,
    slaInfo: null,
    engineOptions: [],
    currentVersion: "",
    clusterConfig: "",
    clusterName: "",
    mainClass: "",
    taskConfig: "",
    currentStatus: "",
    dslConfig: "",
    jarUrl: "",
  });
  const [loading, setLoading] = useState<{
    edit: boolean;
    del: boolean;
    start: boolean;
    reset: boolean;
  }>({
    edit: false,
    del: false,
    start: false,
    reset: false,
  });
  const [visible, setVisible] = useState<{
    record: boolean;
    newPlan: string;
    newRuntimeConfig: string;
  }>({
    record: false,
    newPlan: "",
    newRuntimeConfig: "",
  });
  const [versionData, setVersionData] = useState({
    version: "",
    versionList: [],
  });

  const typeMean = {
    INTEGRATE: "集成",
    DISTRIBUTE: "分发",
    PROCESS: "计算",
    SERVE: "服务",
    STAT: "统计",
    CUSTOM: "自定义",
  };

  useEffect(() => {
    if (jobItem) {
      const { engineVersion, clusterArgs, mainClass, args } = jobItem;
      getTaskIdStatus(jobItem.id, { refresh: false }).then((res) => {
        setState({
          ...state,
          currentStatus: res.data,
          detailInfo: jobItem,
          engineOptions: state.engineOptions,
          currentVersion: engineVersion,
          clusterConfig: clusterArgs,
          mainClass,
          taskConfig: args,
        });
      });
    }
    getApiVersions().then((res) => {
      setVersionData({
        ...versionData,
        versionList: res,
        version: jobItem?.release?.versionName,
      });
    });
  }, [jobItem]);

  const { detailInfo, currentStatus } = state;
  const { id, jobName, release } = detailInfo || {};
  const { job, clusterName, clusterConfig, jobConfig } = release || {};

  const handleClickMoreOperator: MenuProps["onClick"] = () => {
    setVisible({
      ...visible,
      record: true,
    });
  };

  const menus = (
    <Menu>
      <Menu.Item onClick={handleClickMoreOperator}>操作记录</Menu.Item>
    </Menu>
  );

  const closeRecord = () => {
    setVisible({
      ...visible,
      record: false,
    });
  };

  const handleResetJob = () => {
    Modal.confirm({
      title: "重置任务",
      icon: <ExclamationCircleOutlined style={{ color: "orange" }} />,
      content: (
        <>
          是否重置名称为 <Tag color="orange">{jobName}</Tag> 的作业？
        </>
      ),
      onOk: async () => {
        setLoading({
          ...loading,
          reset: true,
        });
        const response = await resetJob(id);
        setLoading({
          ...loading,
          reset: false,
        });
        if (response) {
          message.success("重置成功");
        }
      },
      okText: "确定",
      cancelText: "取消",
    });
  };

  const handleStatus = () => {
    getTaskIdStatus(id, { refresh: false }).then((res) => {
      setState({
        ...state,
        currentStatus: res.data,
      });
    });
  };

  useEffect(() => {
    const getStatus = window.setInterval(() => {
      handleStatus();
    }, 8000);
    return () => {
      getStatus && window.clearInterval(getStatus);
    };
  }, [id]);

  const handleSubmitStart = async () => {
    setLoading({
      ...loading,
      start: true,
    });
    getOperations(id, { action: "start" }).then((res) => {
      if (res.success) {
        message.success("启动成功");
        handleStatus();
      }
      setLoading({
        ...loading,
        start: false,
      });
    });
  };
  const handleSubmit = async () => {
    const values = await form.validateFields();
    const cluster = {};
    const job = {};
    if (!isEmpty(values?.clusterConfig?.config)) {
      for (const item of values?.clusterConfig?.config) {
        cluster[item.key] = item.value;
      }
    }
    if (!isEmpty(values?.jobConfig?.config)) {
      for (const item of values?.jobConfig?.config) {
        job[item.key] = item.value;
      }
    }

    getReleases(uniqueId, {
      newJobConfig: isEmpty(job) ? jobConfig : job,
      newClusterConfig: isEmpty(cluster) ? clusterConfig : cluster,
      versionName: versionData.version,
    }).then((res) => {
      if (res.success) {
        message.success("保存成功");
      }
    });
  };

  const handleSubmitDraft = async () => {
    setLoading({
      ...loading,
      start: true,
    });
    const values = await form.validateFields();
    const cluster = {};
    const job = {};
    if (!isEmpty(values?.clusterConfig?.config)) {
      for (const item of values?.clusterConfig?.config) {
        cluster[item.key] = item.value;
      }
    }
    if (!isEmpty(values?.jobConfig?.config)) {
      for (const item of values?.jobConfig?.config) {
        job[item.key] = item.value;
      }
    }

    getReleases(uniqueId, {
      newJobConfig: isEmpty(job) ? jobConfig : job,
      newClusterConfig: isEmpty(cluster) ? clusterConfig : cluster,
      versionName: versionData.version,
    }).then((res) => {
      if (res.success) {
        getOperations(id, { action: "start" }).then((res) => {
          if (res.success) {
            message.success("提交成功");
            handleStatus();
          }
        });
      }
    });
    setLoading({
      ...loading,
      start: false,
    });
  };

  const handleRefresh = async () => {
    getTaskIdStatus(id, { refresh: true }).then((res) => {
      setState({
        ...state,
        currentStatus: res.data,
      });
    });
  };

  const handleStopJob = async () => {
    setLoading({
      ...loading,
      del: true,
    });
    getOperations(id, { action: "stop" }).then((res) => {
      if (res.success) {
        message.success("停止成功");
        getTaskIdStatus(id, { refresh: false }).then((res) => {
          setState({
            ...state,
            currentStatus: res.data,
          });
        });
      }
      setLoading({
        ...loading,
        del: false,
      });
    });
  };

  const syncDetailConfig = (params: any) => {
    setState({
      ...state,
      ...params,
    });
  };
  const OptionButtonGroup = () => {
    return (
      <Space>
        {currentStatus === "CREATED" && (
          <Button
            type="primary"
            onClick={handleSubmitDraft}
            loading={loading.start}
          >
            提交
          </Button>
        )}
        {currentStatus === "CREATED" && (
          <Button type="primary" onClick={handleSubmit}>
            保存
          </Button>
        )}

        {["FAILED", "STOPPED"].includes(currentStatus) && (
          <Button
            type="primary"
            onClick={handleSubmitStart}
            loading={loading.start}
          >
            启动
          </Button>
        )}

        {["WAITING", "RUNNING"].includes(currentStatus) && (
          <Button onClick={handleStopJob} loading={loading.del}>
            停止
          </Button>
        )}
        <>
          <Button onClick={handleResetJob} disabled>
            重置
          </Button>
        </>
        <Dropdown overlay={menus}>
          <Button onClick={(e) => e.preventDefault()}>...</Button>
        </Dropdown>
      </Space>
    );
  };

  return (
    <div>
      <Card title="基本信息" extra={<OptionButtonGroup />}>
        <Row style={{ marginBottom: 24 }}>
          <Col span={6}>实例名称：{job?.instanceName}</Col>
          <Col span={6}>作业ID：{id}</Col>
          <Col span={6}>任务类型：{job?.type && typeMean[job.type]}</Col>
          <Col span={6}>
            状态：
            <Tag color={STATUS_COLOR_MAPPING[currentStatus] || currentStatus}>
              {currentStatus}
            </Tag>
            <ReloadOutlined onClick={handleRefresh} />
          </Col>
        </Row>
        <Row style={{ marginBottom: 24 }}>
          <Col span={6}>图名称：{job?.graphs[0]?.name}</Col>
          <Col span={6}>任务名称：{job?.name}</Col>
          <Col span={6}>集群名称：{clusterName}</Col>
          <Col span={6}>
            引擎版本：
            <Select
              value={versionData.version}
              disabled={currentStatus !== "CREATED"}
              onChange={(value) => {
                setVersionData({ ...versionData, version: value });
              }}
            >
              {versionData.versionList?.map((item) => {
                return (
                  <Select.Option value={item.name}>{item.name}</Select.Option>
                );
              })}
            </Select>
          </Col>
        </Row>
        <Form form={form}>
          <BasicTabs
            stageType={currentStatus}
            record={detailInfo}
            jobItem={jobItem}
            syncConfig={syncDetailConfig}
            form={form}
          />
        </Form>
      </Card>
      {visible.record && (
        <OperationRecord
          visible={visible.record}
          onClose={closeRecord}
          jobId={id}
        />
      )}
    </div>
  );
};
export default BasicInfo;
