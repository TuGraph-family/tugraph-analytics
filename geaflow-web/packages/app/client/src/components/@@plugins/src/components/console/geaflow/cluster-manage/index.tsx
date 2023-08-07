import React, { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Input,
  Modal,
  Button,
  Form,
  message,
  Space,
  Popconfirm,
  Table,
} from "antd";
import { ClusterConfigPanel } from "./configPanel";
import styles from "./list.module.less";
import {
  getClustersList,
  updateClusters,
  deleteClusters,
} from "../services/cluster";
import CreateCluster from "./create";
import $i18n from "../../../../../../i18n";

const { Search } = Input;

export const ClusterManage: React.FC<{}> = ({}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    list: [],
    originList: [],
    visible: false,
    item: {},
    name: "",
    search: "",
  });

  const [createState, setCreateState] = useState({
    visible: false,
  });

  const { t } = useTranslation();
  const handelResources = async () => {
    const respData = await getClustersList({ name: state.search });
    setState({
      ...state,
      list: respData,
      originList: respData,
      visible: false,
    });
  };

  useEffect(() => {
    handelResources();
  }, [state.search]);

  const showCurrentClusterConfigInfo = (record) => {
    let defaultFormValues = {};
    // 根据 currentItem 来判断是新增还是修改
    const configArr = [];
    for (const key in record.config) {
      const current = record.config[key];
      configArr.push({
        key,
        value: current,
      });
    }
    defaultFormValues = {
      clusterConfig: {
        type: record.type,
        config: configArr,
      },
    };
    setState({
      ...state,
      visible: true,
      item: defaultFormValues,
      name: record.name,
    });
  };

  const columns = [
    {
      title: t("i18n.key.cluster.name"),
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <span>{record.name}</span>

          <br />
          {record?.comment && (
            <span style={{ fontSize: 12, color: "#ccc" }}>
              {record.comment}
            </span>
          )}
        </span>
      ),
    },

    // {
    //   title: "集群名",
    //   dataIndex: "name",
    //   key: "name",
    // },
    {
      title: t("i18n.key.cluster.type"),
      dataIndex: "type",
      key: "type",
    },
    {
      title: t("i18n.key.operator"),
      key: "creatorName",
      width: 150,
      render: (_, record: any) => (
        <span>
          {t("i18n.key.creator")}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {t("i18n.key.modifier")}
              {record.modifierName}
            </span>
          )}
        </span>
      ),
    },
    {
      title: t("i18n.key.operation.time"),
      key: "createTime",
      width: 300,
      render: (_, record: any) => (
        <span>
          {t("i18n.key.creation.time")}：{record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {t("i18n.key.modification.time")}：{record.modifyTime}
            </span>
          )}
        </span>
      ),
    },
    {
      title: t("i18n.key.Operation"),
      dataIndex: "container",
      key: "container",
      hideInSearch: true,
      render: (_, record) => {
        return (
          <Space>
            <a onClick={() => showCurrentClusterConfigInfo(record)}>
              {t("i18n.key.Edit")}
            </a>
            <Popconfirm
              title={t("i18n.key.confirm.deletion")}
              onConfirm={() => {
                deleteClusters(record?.name).then((res) => {
                  if (res?.success) {
                    message.success(t("i18n.key.deletion.succeeded"));
                    handelResources();
                  }
                });
              }}
              okText={t("i18n.key.confirm")}
              cancelText={t("i18n.key.cancel")}
            >
              <a>{t("i18n.key.delete")}</a>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  const handleCloseModal = () => {
    setState({
      ...state,
      visible: false,
      item: {},
    });
  };
  const handleOkModal = () => {
    form.validateFields().then((values) => {
      const { clusterConfig = {} } = values;
      const { type, config = [] } = clusterConfig;
      const configObj = {
        type,
        config: {},
      };
      for (const item of config) {
        configObj.config[item.key] = item.value;
      }

      updateClusters(state.name, configObj).then((res) => {
        if (res?.code) {
          message.success(t("i18n.key.update.succeeded"));
          handelResources();
        } else {
          message.error(res?.message);
        }
      });
    });
  };

  return (
    <div className={styles["example-table"]}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <div style={{ fontWeight: 500, fontSize: 16 }}>
          {$i18n.get({
            id: "openpiece-geaflow.job-detail.components.basicInfo.ClustersList",
            dm: "集群列表",
          })}
        </div>
        <div>
          <Search
            placeholder={t("i18n.key.search.keywords")}
            onSearch={(value) => {
              setState({
                ...state,
                search: value,
              });
            }}
            style={{ width: 286, marginRight: 16 }}
          />
          <Button
            key="button"
            onClick={() => {
              setCreateState({
                ...createState,
                visible: true,
              });
            }}
            type="primary"
          >
            {t("i18n.key.add")}
          </Button>
        </div>
      </div>
      <Table
        columns={columns}
        dataSource={state.list}
        pagination={{
          showQuickJumper: true,
          hideOnSinglePage: true,
        }}
      />
      <Modal
        title={t("i18n.key.configuration.parameters")}
        width={800}
        visible={state.visible}
        onCancel={handleCloseModal}
        onOk={handleOkModal}
        cancelText={t("i18n.key.cancel")}
        okText={t("i18n.key.confirm")}
      >
        <Form form={form}>
          <ClusterConfigPanel
            prefixName="clusterConfig"
            values={state.item}
            form={form}
          />
        </Form>
      </Modal>

      <CreateCluster
        visible={createState.visible}
        reload={() => {
          setCreateState({
            ...createState,
            visible: false,
          });
          handelResources();
        }}
        close={() => {
          setCreateState({
            ...createState,
            visible: false,
          });
        }}
      />
    </div>
  );
};
