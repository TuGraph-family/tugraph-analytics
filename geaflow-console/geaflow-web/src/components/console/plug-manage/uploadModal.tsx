import { Modal, Form, Input, message, Upload, Select, Radio } from "antd";
import { UploadOutlined } from "@ant-design/icons";
import React, { useEffect, useState } from "react";
import { createPlugins, getRemoteFiles } from "../services/plugin-manage";
import { InboxOutlined } from "@ant-design/icons";
import { isEmpty } from "lodash";
import $i18n from "@/components/i18n";

const { Dragger } = Upload;
interface AddTemplateProps {
  isAddMd?: boolean;
  instanceName?: string;
  onLoad?: () => void;
  onclose?: () => void;
  files?: any;
}

export const AddTemplateModal: React.FC<AddTemplateProps> = ({
  isAddMd,
  onclose,
  instanceName,
  onLoad,
  files,
}) => {
  const [form] = Form.useForm();
  const [state, setState] = useState({
    loading: false,
    fileList: [],
    value: 1,
  });
  useEffect(() => {}, []);
  const handleCancel = () => {
    onclose();
    form.resetFields();
  };
  const handleOk = async () => {
    const values = await form.validateFields();
    setState({
      ...state,
      loading: true,
    });
    const { functionFile, name, type, category, fileId } = values;

    const formData = new FormData();

    !isEmpty(functionFile) &&
      functionFile.forEach((item) => {
        formData.append("jarFile", item.originFileObj);
      });
    formData.append("name", name);
    formData.append("type", type);
    category && formData.append("category", category);
    fileId && formData.append("fileId", fileId);
    const resp = await createPlugins(formData);

    setState({
      ...state,
      loading: false,
    });
    if (resp?.success) {
      message.success(
        $i18n.get({
          id: "openpiece-geaflow.geaflow.function-manage.uploadModal.UploadedSuccessfully",
          dm: "上传成功",
        })
      );
      onLoad();
      form.resetFields();
    } else {
      message.error(resp?.message);
    }
  };

  const normFile = (e: any) => {
    if (Array.isArray(e)) {
      return e;
    }
    setState({ ...state, fileList: e?.fileList });
    return e?.fileList.slice(-1);
  };

  const props = {
    beforeUpload: (file) => {
      return false;
    },
  };

  return (
    <Modal
      title={$i18n.get({
        id: "openpiece-geaflow.geaflow.function-manage.uploadModal.AddPlugin",
        dm: "新增插件",
      })}
      visible={isAddMd}
      onOk={handleOk}
      onCancel={handleCancel}
      confirmLoading={state.loading}
      width={700}
      okText={$i18n.get({
        id: "openpiece-geaflow.geaflow.function-manage.uploadModal.Confirm",
        dm: "确认",
      })}
      cancelText={$i18n.get({
        id: "openpiece-geaflow.geaflow.function-manage.uploadModal.Cancel",
        dm: "取消",
      })}
    >
      <Form form={form}>
        <Form.Item
          name="name"
          label={$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.uploadModal.Name",
            dm: "名称",
          })}
          rules={[
            {
              required: true,
              message: $i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EnterAPluginName",
                dm: "请输入插件名称",
              }),
            },
          ]}
        >
          <Input
            placeHolder={$i18n.get({
              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EnterAPluginName",
              dm: "请输入插件名称",
            })}
          />
        </Form.Item>

        <Form.Item
          name="type"
          label={$i18n.get({
            id: "openpiece-geaflow.geaflow.plugin-manage.pluginType",
            dm: "插件类型",
          })}
          rules={[
            {
              required: true,
              message: $i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EnterAPluginType",
                dm: "请输入插件类型",
              }),
            },
          ]}
        >
          <Input
            placeholder={$i18n.get({
              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EnterAPluginType",
              dm: "请输入插件类型",
            })}
          />
        </Form.Item>
        <Form.Item
          name="category"
          initialValue={"TABLE"}
          label={$i18n.get({
            id: "openpiece-geaflow.geaflow.plugin-manage.pluginCategory",
            dm: "插件种类",
          })}
          rules={[
            {
              required: true,
              message: $i18n.get({
                id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EnterAPluginCategory",
                dm: "请选择插件种类",
              }),
            },
          ]}
        >
          <Select disabled>
            <Select.Option value="TABLE">TABLE</Select.Option>
          </Select>
        </Form.Item>

        {/* <Form.Item
          name="comment"
          label={$i18n.get({
            id: "openpiece-geaflow.geaflow.function-manage.uploadModal.Description",
            dm: "描述",
          })}
        >
          <Input.TextArea
            placeHolder={$i18n.get({
              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.EnterADescription",
              dm: "请输入描述",
            })}
          />
        </Form.Item> */}
        <Form.Item name="radio" initialValue={state.value}>
          <Radio.Group
            onChange={(e) => {
              setState({ ...state, value: e.target.value });
            }}
            value={state.value}
          >
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
          </Radio.Group>
        </Form.Item>
        {state.value === 1 ? (
          <Form.Item
            label={$i18n.get({
              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.JarFile",
              dm: "Jar文件",
            })}
          >
            <Form.Item
              name="functionFile"
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
        ) : (
          <Form.Item
            name="fileId"
            label={$i18n.get({
              id: "openpiece-geaflow.geaflow.function-manage.uploadModal.ExistingFiles",
              dm: "已有文件",
            })}
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
      </Form>
    </Modal>
  );
};
