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
import $i18n from "../../../../../../i18n";

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
    INTEGRATE: $i18n.get({
      id: "openpiece-geaflow.geaflow.computing.Integration",
      dm: "集成",
    }),
    DISTRIBUTE: $i18n.get({
      id: "openpiece-geaflow.geaflow.computing.Distribution",
      dm: "分发",
    }),
    PROCESS: $i18n.get({
      id: "openpiece-geaflow.geaflow.computing.Calculation",
      dm: "计算",
    }),
    SERVE: $i18n.get({
      id: "openpiece-geaflow.geaflow.computing.Service",
      dm: "服务",
    }),
    STAT: $i18n.get({
      id: "openpiece-geaflow.geaflow.computing.Statistics",
      dm: "统计",
    }),
    CUSTOM: $i18n.get({
      id: "openpiece-geaflow.geaflow.computing.Custom",
      dm: "自定义",
    }),
  };

  const columns = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.TaskName",
        dm: "任务名称",
      }),
      dataIndex: "name",
      key: "name",
      width: 120,
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.TaskType",
        dm: "任务类型",
      }),
      dataIndex: "type",
      key: "type",
      width: 100,
      render: (_, record: any) => <span>{typeMean[record.type]}</span>,
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.GraphList",
        dm: "图列表",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.TableList",
        dm: "表列表",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.FunctionList",
        dm: "函数列表",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      width: 120,
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.computing.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.computing.ModifiedByRecordmodifiername",
                  dm: "修改人：{recordModifierName}",
                },
                { recordModifierName: record.modifierName }
              )}
            </span>
          )}
        </span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      width: 250,
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.computing.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.computing.ModificationTimeRecordmodifytime",
                  dm: "修改时间：{recordModifyTime}",
                },
                { recordModifyTime: record.modifyTime }
              )}
            </span>
          )}
        </span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.computing.Operation",
        dm: "操作",
      }),
      width: 200,
      fixed: "right",
      render: (_, record: any) => (
        <Space>
          <a
            onClick={() => {
              setInstance({ ...instance, instanceList: record, check: false });
              form.setFieldsValue(record);
              setIsModalOpen(true);
            }}
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.Edit",
              dm: "编辑",
            })}
          </a>
          <a
            onClick={() => {
              message.info(
                $i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.PublishingIsInProgressPlease",
                  dm: "正在发布中请稍后",
                })
              );
              getJobsReleases(record?.id).then((res) => {
                if (res.success) {
                  message.success(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.PublishedSuccessfully",
                      dm: "发布成功",
                    })
                  );
                  window.location.href = `${redirectUrl}?uniqueId=${record?.id}`;
                }
              });
            }}
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.Publish",
              dm: "发布",
            })}
          </a>
          <a
            onClick={() => {
              getJobsTasks(record?.id).then((res) => {
                if (isEmpty(res)) {
                  message.info(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.TheTaskHasNotBeen",
                      dm: "任务未发布，请先发布",
                    })
                  );
                } else {
                  window.location.href = `${redirectUrl}?uniqueId=${record?.id}`;
                }
              });
            }}
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.ViewJobs",
              dm: "查看作业",
            })}
          </a>
          <Popconfirm
            title={$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.AreYouSureYouWant",
              dm: "确认删除？",
            })}
            onConfirm={() => {
              deleteComputing(record?.id).then((res) => {
                if (res?.success) {
                  message.success(
                    $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.DeletedSuccessfully",
                      dm: "删除成功",
                    })
                  );
                  handelTemplata();
                }
              });
            }}
            okText={$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.Ok",
              dm: "确定",
            })}
            cancelText={$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.Cancel",
              dm: "取消",
            })}
          >
            <a>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.computing.Delete",
                dm: "删除",
              })}
            </a>
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
            message.success(
              $i18n.get({
                id: "openpiece-geaflow.geaflow.computing.EditedSuccessfully",
                dm: "编辑成功",
              })
            );
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
            message.success(
              $i18n.get({
                id: "openpiece-geaflow.geaflow.computing.AddedSuccessfully",
                dm: "新增成功",
              })
            );
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
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.GraphCalculationList",
                  dm: "图任务",
                })}
              </a>
            </Breadcrumb.Item>
            <Breadcrumb.Item>
              {instance.instanceList?.id
                ? instance.check
                  ? $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.FigureCalculationDetails",
                      dm: "图任务详情",
                    })
                  : $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.EditGraphCalculation",
                      dm: "编辑图任务",
                    })
                : $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.AddGraphCalculation",
                    dm: "新增图任务",
                  })}
            </Breadcrumb.Item>
          </Breadcrumb>

          <p style={{ fontWeight: 500, fontSize: 20, marginTop: 8 }}>
            {instance.instanceList?.id
              ? instance.check
                ? $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.FigureCalculationDetails",
                    dm: "图任务详情",
                  })
                : $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.EditGraphCalculation",
                    dm: "编辑图任务",
                  })
              : $i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.AddGraphCalculation",
                  dm: "新增图任务",
                })}
          </p>
          <div className={styles["definition-form"]}>
            <Form form={form}>
              <Form.Item
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.TaskName",
                  dm: "任务名称",
                })}
                name="name"
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.EnterATaskName",
                      dm: "请输入任务名称",
                    }),
                  },
                ]}
                initialValue=""
              >
                <Input disabled={instance.check} />
              </Form.Item>

              <Form.Item
                label={$i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.TaskDescription",
                  dm: "任务描述",
                })}
                name="comment"
              >
                <Input.TextArea disabled={instance.check} />
              </Form.Item>
              <Form.Item
                label="DSL"
                name="userCode"
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.computing.EnterDsl",
                      dm: "请输入DSL",
                    }),
                  },
                ]}
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
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.Cancel",
                  dm: "取消",
                })}
              </Button>
              {!instance.check && (
                <Button
                  className={styles["bottom-btn"]}
                  type="primary"
                  htmlType="submit"
                  onClick={handleOk}
                  loading={loading}
                >
                  {$i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.Submit",
                    dm: "提交",
                  })}
                </Button>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div>
          <p>
            <span className={styles["definition-title"]}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.computing.GraphCalculation",
                dm: "图任务",
              })}
            </span>
            <span className={styles["meaing"]}>
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.computing.DefinesTheDataProcessingLogic",
                dm: "定义图计算的数据处理逻辑。",
              })}
            </span>
          </p>
          <div className={styles["definition-table"]}>
            <div className={styles["definition-header"]}>
              <div className={styles["title"]}>
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.GraphCalculationList",
                  dm: "图任务列表",
                })}
              </div>
              <div>
                <Search
                  style={{ width: 286, marginRight: 16 }}
                  placeholder={$i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.EnterASearchKeyword",
                    dm: "请输入搜索关键词",
                  })}
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
                  {$i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.Add",
                    dm: "新增",
                  })}
                </Button>
              </div>
            </div>
            <Table
              dataSource={temeplateList}
              columns={columns}
              pagination={{ pageSize: 10 }}
              scroll={{ x: 1000 }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
