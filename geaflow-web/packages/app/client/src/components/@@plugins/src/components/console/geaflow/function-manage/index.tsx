import React, { useEffect, useState } from "react";
import {
  Input,
  Modal,
  Tag,
  Table,
  Button,
  Space,
  message,
  Popconfirm,
  Descriptions,
} from "antd";
import {
  deleteFunction,
  getFunctions,
  getRemoteFiles,
} from "../services/function-manage";
import { AddTemplateModal } from "./uploadModal";
import $i18n from "../../../../../../i18n";

const { Search } = Input;

export const GeaflowFunctionManage: React.FC<{}> = ({}) => {
  const [state, setState] = useState({
    functionManage: [],
    search: "",
    isAddMd: false,
    files: [],
    visible: false,
    item: {},
  });
  const currentInstance = localStorage.getItem("GEAFLOW_CURRENT_INSTANCE")
    ? JSON.parse(localStorage.getItem("GEAFLOW_CURRENT_INSTANCE"))
    : {};
  const instanceName = currentInstance.value;
  const handelResources = async () => {
    if (instanceName) {
      const respData = await getFunctions(instanceName, state.search);
      const files = await getRemoteFiles();
      setState({
        ...state,
        functionManage: respData,
        files,
        isAddMd: false,
      });
    }
  };

  useEffect(() => {
    handelResources();
  }, [state.search, instanceName]);

  const handleDelete = async (record) => {
    const resp = await deleteFunction(instanceName, record.name);
    if (resp) {
      message.success(
        $i18n.get({
          id: "openpiece-geaflow.geaflow.function-manage.DeletedSuccessfully",
          dm: "删除成功",
        })
      );
      handelResources();
    }
  };
  const showCurrentClusterConfigInfo = (record) => {
    console.log(record);
    setState({
      ...state,
      visible: true,
      item: record.jarPackage,
    });
  };

  const columns = [
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.function-manage.FunctionName",
        dm: "函数名称",
      }),
      dataIndex: "name",
      key: "name",
      render: (_, record: any) => (
        <span>
          <a onClick={() => showCurrentClusterConfigInfo(record)}>
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
      title: "EntryClass",
      dataIndex: "entryClass",
      key: "entryClass",
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.function-manage.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.function-manage.ModifiedByRecordmodifiername",
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
        id: "openpiece-geaflow.geaflow.function-manage.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.function-manage.ModificationTimeRecordmodifytime",
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
        id: "openpiece-geaflow.geaflow.function-manage.Operation",
        dm: "操作",
      }),
      dataIndex: "container",
      key: "container",
      hideInSearch: true,
      render: (_, record) => {
        return (
          <Space>
            <Popconfirm
              title={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.AreYouSureYouWant",
                dm: "确定删除该记录吗？",
              })}
              onConfirm={() => handleDelete(record)}
              okText={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.Ok",
                dm: "确定",
              })}
              cancelText={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.Cancel",
                dm: "取消",
              })}
            >
              <a>
                {$i18n.get({
                  id: "openpiece-geaflow.geaflow.function-manage.Delete",
                  dm: "删除",
                })}
              </a>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  const { item } = state;
  return (
    <>
      <p>
        <span style={{ fontSize: 20, fontWeight: 500 }}>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.function-definition.FunctionDefinition",
            dm: "函数定义",
          })}
        </span>
        <span
          style={{
            fontWeight: 400,
            fontSize: 14,
            color: "#98989d",
            marginLeft: 8,
          }}
        >
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.function-definition.DefinesTheFunctionStructureOn",
            dm: "定义图计算任务依赖的函数结构。",
          })}
        </span>
      </p>
      <div style={{ padding: 16, background: "#fff" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            marginBottom: 16,
          }}
        >
          <div style={{ fontWeight: 500, fontSize: 16 }}>
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.function-definition.FunctionDefinitionList",
              dm: "函数列表",
            })}
          </div>
          <div>
            <Search
              style={{ width: 286, marginRight: 16 }}
              placeholder={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.EnterASearchKeyword",
                dm: "请输入搜索关键词",
              })}
              onSearch={(value) => {
                setState({ ...state, search: value });
              }}
            />

            <Button
              onClick={() => {
                setState({
                  ...state,
                  isAddMd: true,
                });
              }}
              type="primary"
            >
              {$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.Add",
                dm: "新增",
              })}
            </Button>
          </div>
        </div>
        <Table
          dataSource={state.functionManage}
          columns={columns}
          pagination={{
            hideOnSinglePage: true,
            showQuickJumper: true,
            size: "small",
          }}
        />

        <AddTemplateModal
          isAddMd={state.isAddMd}
          onclose={() => {
            setState({
              ...state,
              isAddMd: false,
            });
          }}
          onLoad={() => {
            handelResources();
          }}
          instanceName={instanceName}
          files={state.files}
        />

        <Modal
          title={$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.FunctionConfigurationParameters",
            dm: "函数配置参数",
          })}
          width={850}
          visible={state.visible}
          onCancel={() => {
            setState({ ...state, visible: false });
          }}
          onOk={() => {
            setState({ ...state, visible: false });
          }}
          okText={$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.Ok",
            dm: "确定",
          })}
        >
          <Descriptions column={2}>
            <Descriptions.Item
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.JarPackageName",
                dm: "JAR包名称",
              })}
            >
              {item?.name}
            </Descriptions.Item>
            <Descriptions.Item
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.Type",
                dm: "类型",
              })}
            >
              {item?.type}
            </Descriptions.Item>
            <Descriptions.Item label="MD5">{item?.md5}</Descriptions.Item>
            <Descriptions.Item
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.Path",
                dm: "路径",
              })}
            >
              {item?.path}
            </Descriptions.Item>
            <Descriptions.Item label="URL">{item?.url}</Descriptions.Item>
            <Descriptions.Item
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.CreatorId",
                dm: "创建人ID",
              })}
            >
              {item?.creatorId}
            </Descriptions.Item>
            <Descriptions.Item
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.CreationTime.1",
                dm: "创建时间",
              })}
            >
              {item?.createTime}
            </Descriptions.Item>
            <Descriptions.Item
              label={$i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.UpdateTime",
                dm: "更新时间",
              })}
            >
              {item?.modifyTime}
            </Descriptions.Item>
          </Descriptions>
        </Modal>
      </div>
    </>
  );
};
