/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useEffect, useState } from "react";
import {
  Input,
  Button,
  Table,
  Breadcrumb,
  Form,
  Space,
  message,
  Popconfirm,
  Tooltip,
} from "antd";
import { useOpenpieceUserAuth } from "@tugraph/openpiece-client";
import {
  getJobsList,
  getJobsReleases,
  deleteComputing,
  getJobsEditList,
  getJobsTasks,
  getRemoteFiles,
} from "../services/computing";
import { isEmpty } from "lodash";
import { history, useSearchParams } from "umi";
import styles from "./index.module.less";
import $i18n from "@/components/i18n";
import CreateCompute from "./create";
import { GraphQuery } from "./graph-query";
import { InstanceList } from "../../../console/geaflow";

const { Search } = Input;

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaFlowComputing: React.FC<PluginPorps> = ({ redirectPath }) => {
  const [form] = Form.useForm();
  const [searchParams, setSearchParams] = useSearchParams();
  const jobId = searchParams.get("jobId");
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const [temeplateList, setTemplateList] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState<boolean>(false);
  const [instance, setInstance] = useState({
    instanceList: {},
    check: false,
    edit: false,
    serve: false,
  });
  const [files, setFiels] = useState([]);

  const handelTemplata = async (name?: string) => {
    if (currentInstance?.key) {
      const result = await getJobsList({
        name: name,
        instanceId: currentInstance.key,
      });

      if (result.code === "FORBIDDEN") {
        // 没有登陆，跳到登录页面
        history.push("/");
        return;
      }
      const files = await getRemoteFiles();
      setFiels(files);
      setTemplateList(result?.data?.list);
    }
  };

  useEffect(() => {
    handelTemplata();
  }, []);

  const getJobDetailInfoById = async (id: string) => {
    const response = await getJobsEditList(id);
    if (response.success) {
      if (searchParams.get("view") === "true") {
        setInstance({ ...instance, instanceList: response.data, check: true });
      } else {
        setInstance({ ...instance, instanceList: response.data, edit: true });
      }
      setIsModalOpen(true);
    }
  };

  useEffect(() => {
    if (jobId) {
      // 通过 jobID 查询详情
      getJobDetailInfoById(jobId);
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
      id: "openpiece-geaflow.geaflow.computing.Process",
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
        id: "openpiece-geaflow.geaflow.computing.JobName",
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
              setInstance({ ...instance, instanceList: record, edit: true });
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
                  history.push(`/console/ColJobDetail?uniqueId=${record?.id}`);
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
                  history.push(`/console/ColJobDetail?uniqueId=${record?.id}`);
                }
              });
            }}
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.ViewJobs",
              dm: "查看作业",
            })}
          </a>
          {record.type === "SERVE" && (
            <a
              onClick={() => {
                setInstance({ ...instance, instanceList: record, serve: true });
                setIsModalOpen(true);
              }}
            >
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.computing.Inquire",
                dm: "查询",
              })}
            </a>
          )}
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

  const handleCancel = () => {
    setIsModalOpen(false);
    setInstance({
      ...instance,
      instanceList: {},
      edit: false,
      check: false,
      serve: false,
    });
    form.resetFields();
  };
  const handleSuccess = () => {
    handelTemplata();
    handleCancel();
  };

  return (
    <div className={styles["definition"]}>
      {isModalOpen ? (
        instance.serve ? (
          <div>
            <Breadcrumb style={{ marginBottom: 16 }}>
              <Breadcrumb.Item>
                <a onClick={handleCancel}>
                  {$i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.GraphCalculationList",
                    dm: "图任务",
                  })}
                </a>
              </Breadcrumb.Item>
              <Breadcrumb.Item>
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.Serve",
                  dm: "图查询",
                })}
              </Breadcrumb.Item>
            </Breadcrumb>
            <GraphQuery instance={instance.instanceList} />
          </div>
        ) : (
          <CreateCompute
            handleCancel={handleCancel}
            instance={instance}
            files={files}
            handleSuccess={handleSuccess}
          />
        )
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
              pagination={{
                hideOnSinglePage: true,
                showQuickJumper: true,
              }}
              scroll={{
                x: 1000,
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
