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
  Select,
  Radio,
  Upload,
  Card,
} from "antd";
import {
  getJobsCreat,
  getJobsEdit,
  getJobs,
  getTablesDefinitionList,
} from "../services/computing";
import { getGraphDefinitionList } from "../services/graphDefinition";
import styles from "./index.module.less";
import $i18n from "@/components/i18n";
import { isEmpty, last } from "lodash";
import { InboxOutlined } from "@ant-design/icons";
import { json } from "@codemirror/lang-json";
import CodeMirror from "@uiw/react-codemirror";
import { GraphDefintionTab } from "./graph-tabs/index";

const CreateCompute = ({ handleCancel, instance, files, handleSuccess }) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState<boolean>(false);
  const [state, setState] = useState({
    serveList: [],
    serves: [],
    fields: [],
    tableList: [],
  });

  const isWay = Form.useWatch("type", form);
  const isRadio = Form.useWatch("radio", form);
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const instanceName = currentInstance.value;

  const handleStructMapping = (value: any, name: string) => {
    return value?.map((item: { name: string; structFieldName: string }) => {
      return {
        tableFieldName: item[name],
        structFieldName: item.structFieldName,
      };
    });
  };

  const handleOk = () => {
    form.validateFields().then((val) => {
      setLoading(true);
      const {
        jarFile,
        comment,
        name,
        userCode,
        type,
        entryClass,
        fileId,
        graphIds,
      } = val;
      let structMappings: {
        tableName: string;
        structName: string;
        fieldMappings: { tableName: string; strcutFieldName: string }[];
      }[] = [];
      const filterFieldName = Object.keys(val).filter(
        (item) => !item.indexOf("tableName")
      );
      filterFieldName?.map((item) => {
        const fieldMappings = Object.values(val)?.filter(
          (str) => str && Object.keys(str).indexOf(item) !== -1
        );

        structMappings.push({
          tableName: val[item],
          structName: val[`structName${last(item)}`],
          fieldMappings: handleStructMapping(fieldMappings, item),
        });
      });

      const { id, instanceId } = instance.instanceList || {};
      const formData = new FormData();
      !isEmpty(jarFile) &&
        jarFile.forEach((item) => {
          formData.append("jarFile", item.originFileObj);
        });
      name && formData.append("name", name);
      userCode && formData.append("userCode", userCode);
      formData.append("instanceId", instanceId || currentInstance.key);
      entryClass && formData.append("entryClass", entryClass);
      type && formData.append("type", type);
      fileId && formData.append("fileId", fileId);
      comment && formData.append("comment", comment);
      graphIds && formData.append("graphIds", [graphIds]);
      type === "INTEGRATE" &&
        formData.append("structMappings", JSON.stringify(structMappings));
      if (id) {
        getJobsEdit(formData, id).then((res) => {
          setLoading(false);
          if (res.success) {
            message.success(
              $i18n.get({
                id: "openpiece-geaflow.geaflow.computing.EditedSuccessfully",
                dm: "编辑成功",
              })
            );
            handleSuccess();
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
        getJobsCreat(formData).then((res) => {
          setLoading(false);
          if (res.success) {
            message.success(
              $i18n.get({
                id: "openpiece-geaflow.geaflow.computing.AddedSuccessfully",
                dm: "新增成功",
              })
            );
            handleSuccess();
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

  useEffect(() => {
    handleInstance();
  }, [instance, instanceName]);

  const handleInstance = async () => {
    let fields;
    let serveList;
    let tableList;
    if (!isEmpty(instance?.instanceList)) {
      fields = await getJobs(instance.instanceList.id);
      form.setFieldsValue(fields?.data);
      form.setFieldsValue({
        graphIds: fields?.data?.graphs[0]?.id,
      });
    }
    if (instanceName) {
      serveList = await getGraphDefinitionList({
        instanceName,
      });

      tableList = await getTablesDefinitionList({
        instanceName,
      });
    }
    const structMappings =
      !isEmpty(fields) && JSON.parse(fields?.data?.structMappings);

    const serves = serveList.filter(
      (item) => item.id === fields?.data?.graphs[0]?.id
    );
    setState({
      ...state,
      serves: serves,
      fields: structMappings,
      tableList: tableList,
      serveList,
    });
  };

  const normFile = (e: any) => {
    if (Array.isArray(e)) {
      return e;
    }
    return e?.fileList.slice(-1);
  };

  const props = {
    beforeUpload: () => {
      return false;
    },
  };

  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
    },
    {
      title: "MD5",
      dataIndex: "md5",
      key: "md5",
    },
    {
      title: "修改时间",
      dataIndex: "modifyTime",
      key: "modifyTime",
    },
  ];

  const initMappings = (graph: any) => {
    const vertexList = graph.vertices?.map((v: any) => {
      const fieldMappings = v.fields.map((field: any) => {
        return { structFieldName: field.name };
      });
      return {
        structName: v.name,
        fieldMappings: fieldMappings,
      };
    });
    const edgeList = graph.edges?.map((e: any) => {
      const fieldMappings = e.fields.map((field: any) => {
        return { structFieldName: field.name };
      });
      return {
        structName: e.name,
        fieldMappings: fieldMappings,
      };
    });
    return [...vertexList, ...edgeList];
  };

  return (
    <div className={styles["definition-create"]}>
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
              id: "openpiece-geaflow.geaflow.computing.JobName",
              dm: "任务名称",
            })}
            name="name"
            rules={[
              {
                required: true,
                message: $i18n.get({
                  id: "openpiece-geaflow.geaflow.computing.EnterAJobName",
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
            name="type"
            label={$i18n.get({
              id: "openpiece-geaflow.geaflow.computing.JobType",
              dm: "任务类型",
            })}
            initialValue={"PROCESS"}
          >
            <Select
              options={[
                {
                  value: "PROCESS",
                  label: $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.Process",
                    dm: "加工",
                  }),
                },
                {
                  value: "CUSTOM",
                  label: $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.Custom",
                    dm: "自定义",
                  }),
                },
                {
                  value: "SERVE",
                  label: $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.Serve",
                    dm: "图查询",
                  }),
                },
                {
                  value: "INTEGRATE",
                  label: $i18n.get({
                    id: "openpiece-geaflow.geaflow.computing.Integrated",
                    dm: "集成",
                  }),
                },
              ]}
              disabled={instance.check || instance.edit}
            />
          </Form.Item>
          {isWay === "PROCESS" && (
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
          )}
          {isWay === "CUSTOM" && (
            <>
              <Form.Item
                name="entryClass"
                label="EntryClass"
                rules={[
                  {
                    required: true,
                    message: $i18n.get({
                      id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EntryClass",
                      dm: "请输入entryClass",
                    }),
                  },
                ]}
              >
                <Input
                  disabled={instance.check}
                  placeholder={$i18n.get({
                    id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EntryClass",
                    dm: "请输入entryClass",
                  })}
                />
              </Form.Item>
              {(instance.check || instance.edit) && isWay === "CUSTOM" && (
                <Form.Item>
                  <Card title="UDF" bordered={false}>
                    <Table
                      columns={columns}
                      dataSource={[instance?.instanceList?.jarPackage]}
                      pagination={false}
                    />
                  </Card>
                </Form.Item>
              )}
              {!instance?.check && (
                <>
                  <Form.Item name="radio" initialValue={1}>
                    <Radio.Group>
                      <Radio value={1}>
                        {$i18n.get({
                          id: "openpiece-geaflow.geaflow.function-manage.uploadModal.UploadFiles",
                          dm: "上传文件",
                        })}
                      </Radio>
                      <Radio value={2}>
                        {$i18n.get({
                          id: "openpiece-geaflow.geaflow.function-manage.uploadModal.BindExistingFiles",
                          dm: "绑定已有文件",
                        })}
                      </Radio>
                      <Radio value={3}>
                        {$i18n.get({
                          id: "openpiece-geaflow.geaflow.function-manage.uploadModal.DoNotUpload",
                          dm: "不上传文件",
                        })}
                      </Radio>
                    </Radio.Group>
                  </Form.Item>
                  {isRadio === 1 && (
                    <Form.Item
                      label={$i18n.get({
                        id: "openpiece-geaflow.geaflow.function-manage.uploadModal.JarFile",
                        dm: "Jar文件",
                      })}
                    >
                      <Form.Item
                        name="jarFile"
                        valuePropName="fileList"
                        getValueFromEvent={normFile}
                        noStyle
                      >
                        <Upload.Dragger {...props}>
                          <p className="ant-upload-drag-icon">
                            <InboxOutlined />
                          </p>
                          <p className="ant-upload-text">
                            {$i18n.get({
                              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.DragOrClickSelectFile",
                              dm: "拖拽或点击选择文件",
                            })}
                          </p>
                          <p className="ant-upload-hint">
                            {$i18n.get({
                              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.OnlyJarFilesAreSupported",
                              dm: "只支持 jar 文件。",
                            })}
                          </p>
                        </Upload.Dragger>
                      </Form.Item>
                    </Form.Item>
                  )}
                </>
              )}

              {isRadio === 2 && (
                <Form.Item
                  name="fileId"
                  label={$i18n.get({
                    id: "openpiece-geaflow.geaflow.function-manage.uploadModal.ExistingFiles",
                    dm: "已有文件",
                  })}
                  rules={[
                    {
                      required: true,
                      message: $i18n.get({
                        id: "openpiece-geaflow.geaflow.function-manage.uploadModal.SelectAnExistingFile",
                        dm: "请选择已有文件",
                      }),
                    },
                  ]}
                >
                  <Select
                    placeholder={$i18n.get({
                      id: "openpiece-geaflow.geaflow.function-manage.uploadModal.SelectAnExistingFile",
                      dm: "请选择已有文件",
                    })}
                    allowClear={true}
                    options={files || []}
                    fieldNames={{
                      label: "name",
                      value: "id",
                    }}
                  />
                </Form.Item>
              )}
            </>
          )}
          {["INTEGRATE", "SERVE"].includes(isWay) && (
            <Form.Item
              name="graphIds"
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.SelectQuery",
                dm: "目标图",
              })}
              rules={[
                {
                  required: true,
                  message: $i18n.get({
                    id: "openpiece-geaflow.geaflow.function-manage.uploadModal.SelectQuery",
                    dm: "请选择图",
                  }),
                },
              ]}
            >
              <Select
                disabled={instance.check || instance.edit}
                onChange={(value: string) => {
                  const serves = state.serveList.filter(
                    (item) => item.id === value
                  );
                  const initialValues = initMappings(serves[0]) || [];
                  setState({ ...state, serves, fields: initialValues });
                }}
              >
                {state.serveList?.map((item) => {
                  return (
                    <Select.Option value={item.id}>{item.name}</Select.Option>
                  );
                })}
              </Select>
            </Form.Item>
          )}
          {isWay === "INTEGRATE" && (
            <GraphDefintionTab
              form={form}
              serveList={state.serves}
              tableList={state.tableList}
              fields={state.fields}
              check={instance.check}
            />
          )}
        </Form>
      </div>
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
  );
};

export default CreateCompute;
