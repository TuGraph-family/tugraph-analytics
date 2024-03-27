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
import styles from "./list.module.less";
import {
  getLanguageList,
  llmsCall,
  deleteLanguage,
} from "../services/languages";
import CreateCluster from "./create";
import $i18n from "@/components/i18n";

const { Search, TextArea } = Input;

export const LanguageManage: React.FC<{}> = ({}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    list: [],
    originList: [],
    visible: false,
    item: {},
    modelId: "",
    search: "",
    loading: false,
  });
  const [createState, setCreateState] = useState({
    visible: false,
    editValue: {},
  });

  const { t } = useTranslation();
  const handelResources = async () => {
    const respData = await getLanguageList({ name: state.search });
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

  const columns = [
    {
      title: t("i18n.key.language.name"),
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <span>{record?.name}</span>

          <br />
          {record?.comment && (
            <span style={{ fontSize: 12, color: "#ccc" }}>
              {record.comment}
            </span>
          )}
        </span>
      ),
    },
    {
      title: t("i18n.key.language.type"),
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
          {record?.creatorName} <br />
          {record?.modifierName && (
            <span>
              {t("i18n.key.modifier")}
              {record?.modifierName}
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
          {t("i18n.key.creation.time")}：{record?.createTime} <br />
          {record?.modifyTime && (
            <span>
              {t("i18n.key.modification.time")}：{record?.modifyTime}
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
            <a
              onClick={() => {
                setCreateState({
                  ...createState,
                  visible: true,
                  editValue: record,
                });
              }}
            >
              {t("i18n.key.Edit")}
            </a>
            <a
              onClick={() => {
                setState({
                  ...state,
                  visible: true,
                  modelId: record?.id,
                });
              }}
            >
              {t("i18n.key.Test")}
            </a>
            <Popconfirm
              title={t("i18n.key.confirm.deletion")}
              onConfirm={() => {
                deleteLanguage(record?.name).then((res) => {
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
    form.resetFields();
    setState({
      ...state,
      visible: false,
    });
  };
  const handleOkModal = () => {
    form.validateFields().then((values) => {
      setState({ ...state, loading: true });
      const { prompt } = values;
      llmsCall({
        prompt,
        modelId: state.modelId,
        saveRecord: false,
        withSchema: false,
      }).then((res) => {
        setState({ ...state, loading: false });
        if (res?.success) {
          form.setFieldsValue({ result: res.data });
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
            id: "openpiece-geaflow.job-detail.components.basicInfo.LanguagesList",
            dm: "模型列表",
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
        title={t("i18n.key.model.test")}
        width={600}
        open={state.visible}
        onCancel={handleCloseModal}
        onOk={handleOkModal}
        cancelText={t("i18n.key.cancel")}
        confirmLoading={state.loading}
        okText={t("i18n.key.confirm")}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            label={t("i18n.key.input")}
            name="prompt"
            rules={[{ required: true }]}
            initialValue=""
          >
            <TextArea rows={4} />
          </Form.Item>
          <Form.Item label={t("i18n.key.result")} name="result">
            <TextArea rows={8} disabled />
          </Form.Item>
        </Form>
      </Modal>

      <CreateCluster
        visible={createState.visible}
        editValue={createState.editValue}
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
            editValue: {},
          });
        }}
      />
    </div>
  );
};
