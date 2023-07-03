import React, { useEffect, useState } from "react";
import {
  Input,
  Button,
  Table,
  Modal,
  Form,
  Space,
  message,
  Popconfirm,
  Breadcrumb,
  Tooltip,
} from "antd";
import { json } from "@codemirror/lang-json";
import CodeMirror from "@uiw/react-codemirror";
import { useOpenpieceUserAuth } from "@tugraph/openpiece-client";
import {
  getJobsList,
  getJobsCreat,
  getJobsEdit,
  getJobsReleases,
  deleteComputing,
  getJobsEditList,
  getJobsTasks,
} from "../services/computing";
import { isEmpty } from "lodash";
import { useHistory } from "umi";
import styles from "./index.module.less";

const { Search } = Input;

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaFlowComputing: React.FC<PluginPorps> = ({ redirectPath }) => {
  const redirectUrl = redirectPath?.[0]?.path || "/";
  const [form] = Form.useForm();
  const history = useHistory();
  const { location } = history;
  const jobId = location.query?.jobId;
  const { redirectLoginURL } = useOpenpieceUserAuth();
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};

  const [temeplateList, setTemplateList] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState<boolean>(false);
  const [instance, setInstance] = useState({
    instanceList: {},
    check: false,
  });
  const [loading, setLoading] = useState<boolean>(false);

  const handelTemplata = async (name?: string) => {
    if (currentInstance?.key) {
      const result = await getJobsList({
        name: name,
        instanceId: currentInstance.key,
      });

      if (result.code === "FORBIDDEN") {
        // 没有登陆，跳到登录页面
        redirectLoginURL();
        return;
      }

      setTemplateList(result?.data?.list);
    }
  };

  useEffect(() => {
    handelTemplata();
  }, []);

  useEffect(() => {
    if (jobId) {
      getJobsEditList(jobId).then((res) => {
        form.setFieldsValue(res.data);
        setIsModalOpen(true);
        setInstance({ ...instance, instanceList: res.data, check: true });
      });
    }
  }, [jobId]);

  const typeMean = {
    INTEGRATE: "集成",
    DISTRIBUTE: "分发",
    PROCESS: "计算",
    SERVE: "服务",
    STAT: "统计",
    CUSTOM: "自定义",
  };

  const columns = [
    {
      title: "任务名称",
      dataIndex: "name",
      key: "name",
      width: 150,
      render: (_, record: any) => (
        <span>
          <a
            onClick={() => {
              setInstance({ ...instance, instanceList: record, check: true });
              form.setFieldsValue(record);
              setIsModalOpen(true);
            }}
          >
            {record.name}
          </a>

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
      title: "任务类型",
      dataIndex: "type",
      key: "type",
      width: 150,
      render: (_, record: any) => <span>{typeMean[record.type]}</span>,
    },
    {
      title: "图列表",
      dataIndex: "graphs",
      key: "graphs",
      width: 150,
      ellipsis: {
        showTitle: false,
      },
      render: (text: any) => {
        if (isEmpty(text)) {
          return "-";
        }
        return (
          <Tooltip>
            {text
              .map((obj) => {
                return obj.name;
              })
              .join(",")}
          </Tooltip>
        );
      },
    },
    {
      title: "表列表",
      dataIndex: "structs",
      key: "structs",
      width: 150,
      ellipsis: {
        showTitle: false,
      },
      render: (text: any) => {
        if (isEmpty(text)) {
          return "-";
        }
        return (
          <Tooltip>
            {text
              .map((obj) => {
                return obj.name;
              })
              .join(",")}
          </Tooltip>
        );
      },
    },
    {
      title: "函数列表",
      dataIndex: "functions",
      key: "functions",
      width: 150,
      ellipsis: {
        showTitle: false,
      },
      render: (text: any) => {
        if (isEmpty(text)) {
          return "-";
        }
        return (
          <Tooltip>
            {text
              .map((obj) => {
                return obj.name;
              })
              .join(",")}
          </Tooltip>
        );
      },
    },
    {
      title: "操作人",
      key: "creatorName",
      width: 150,
      render: (_, record: any) => (
        <span>
          创建人：{record.creatorName} <br />
          {record?.modifierName && <span>修改人：{record.modifierName}</span>}
        </span>
      ),
    },
    {
      title: "操作时间",
      key: "createTime",
      width: 300,
      render: (_, record: any) => (
        <span>
          创建时间：{record.createTime} <br />
          {record?.modifyTime && <span>修改时间：{record.modifyTime}</span>}
        </span>
      ),
    },

    {
      title: "操作",
      width: 250,
      align: "center",
      render: (_, record: any) => (
        <Space>
          <a
            onClick={() => {
              setInstance({ ...instance, instanceList: record, check: false });
              form.setFieldsValue(record);
              setIsModalOpen(true);
            }}
          >
            编辑
          </a>
          <a
            onClick={() => {
              message.info("正在发布中请稍后");
              getJobsReleases(record?.id).then((res) => {
                if (res.success) {
                  message.success("发布成功");
                  window.location.href = `${redirectUrl}?uniqueId=${record?.id}`;
                }
              });
            }}
          >
            发布
          </a>
          <a
            onClick={() => {
              getJobsTasks(record?.id).then((res) => {
                if (isEmpty(res)) {
                  message.info("任务未发布，请先发布");
                } else {
                  window.location.href = `${redirectUrl}?uniqueId=${record?.id}`;
                }
              });
            }}
          >
            查看作业
          </a>
          <Popconfirm
            title="确认删除？"
            onConfirm={() => {
              deleteComputing(record?.id).then((res) => {
                if (res?.success) {
                  message.success("删除成功");
                  handelTemplata();
                }
              });
            }}
            okText="确定"
            cancelText="取消"
          >
            <a>删除</a>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const handleOk = () => {
    form.validateFields().then((val) => {
      setLoading(true);

      const { comment, name, userCode } = val;
      const { id, instanceId } = instance.instanceList || {};
      if (id) {
        getJobsEdit(
          { comment, name, userCode, type: "PROCESS", instanceId },
          id
        ).then((res) => {
          setLoading(false);
          if (res.success) {
            message.success("编辑成功");
            handelTemplata();
            setIsModalOpen(false);
            setInstance({ ...instance, instanceList: {} });
            form.resetFields();
          } else {
            message.error(
              <p
                style={{
                  whiteSpace: "pre-line",
                  textAlign: "left",
                }}
              >
                {res?.message}
              </p>
            );
          }
        });
      } else {
        // 获取选择的实例 ID
        const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
          ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
          : {};
        getJobsCreat({
          comment,
          name,
          userCode,
          type: "PROCESS",
          instanceId: currentInstance.key,
        }).then((res) => {
          setLoading(false);
          if (res.success) {
            message.success("新增成功");
            handelTemplata();
            setIsModalOpen(false);
          } else {
            message.error(
              <p
                style={{
                  whiteSpace: "pre-line",
                  textAlign: "left",
                }}
              >
                {res?.message}
              </p>
            );
          }
        });
      }
    });
  };

  const handleCancel = () => {
    setIsModalOpen(false);
    setInstance({ ...instance, instanceList: {} });
    form.resetFields();
  };
  return (
    <div className={styles["definition"]}>
      {isModalOpen ? (
        <div className={styles["definition-create"]}>
          <Breadcrumb style={{ marginBottom: 16 }}>
            <Breadcrumb.Item>
              <a
                onClick={() => {
                  setIsModalOpen(false);
                  form.resetFields();
                  setInstance({ ...instance, instanceList: {} });
                }}
              >
                图计算
              </a>
            </Breadcrumb.Item>
            <Breadcrumb.Item>
              {instance.instanceList?.id
                ? instance.check
                  ? "图计算详情"
                  : "编辑图计算"
                : "新增图计算"}
            </Breadcrumb.Item>
          </Breadcrumb>

          <div className={styles["definition-form"]}>
            <Form form={form}>
              <Form.Item
                label="任务名称"
                name="name"
                rules={[{ required: true, message: "请输入任务名称" }]}
                initialValue=""
              >
                <Input disabled={instance.check} />
              </Form.Item>

              <Form.Item label="任务描述" name="comment">
                <Input.TextArea disabled={instance.check} />
              </Form.Item>
              <Form.Item
                label="DSL"
                name="userCode"
                rules={[{ required: true, message: "请输入DSL" }]}
              >
                <CodeMirror
                  value=""
                  extensions={[json()]}
                  readOnly={instance.check}
                />
              </Form.Item>
            </Form>

            <div className={styles["definition-bottom"]}>
              <Button className={styles["bottom-btn"]} onClick={handleCancel}>
                取消
              </Button>
              {!instance.check && (
                <Button
                  className={styles["bottom-btn"]}
                  type="primary"
                  htmlType="submit"
                  onClick={handleOk}
                  loading={loading}
                >
                  提交
                </Button>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div>
          <p>
            <span className={styles["definition-title"]}>图计算</span>
            <span className={styles["meaing"]}>
              定义图计算任务的数据处理逻辑。
            </span>
          </p>
          <div className={styles["definition-table"]}>
            <div className={styles["definition-header"]}>
              <div className={styles["title"]}>图计算列表</div>
              <div>
                <Search
                  style={{ width: 286, marginRight: 16 }}
                  placeholder="请输入搜索关键词"
                  onSearch={(value) => {
                    handelTemplata(value);
                  }}
                />
                <Button
                  type="primary"
                  onClick={() => {
                    setIsModalOpen(true);
                  }}
                >
                  新增
                </Button>
              </div>
            </div>
            <Table
              dataSource={temeplateList}
              columns={columns}
              pagination={{ pageSize: 10 }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
