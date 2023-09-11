import React, { useEffect, useRef } from "react";
import {
  Form,
  Select,
  Space,
  Input,
  Button,
  Tag,
  InputRef,
  Divider,
  FormInstance,
  Alert,
  message,
  Spin,
} from "antd";
import { MinusCircleOutlined, PlusOutlined } from "@ant-design/icons";
import { useTranslation } from "react-i18next";
import {
  getPluginCategoriesByType,
  getPluginCategoriesConfig,
} from "../services/quickInstall";
import { useImmer } from "use-immer";

interface ICategory {
  type: string;
  config: {};
  category: string;
  comment: string;
  name: string;
}

interface IProps {
  prefixName: string;
  values: ICategory;
  form: FormInstance;
  tipsInfo?: string;
}

const DEFAULT_CATEGORY = {
  runtimeClusterConfig: "RUNTIME_CLUSTER",
  dataConfig: "DATA",
  remoteFileConfig: "REMOTE_FILE",
  runtimeMetaConfig: "RUNTIME_META",
  haMetaConfig: "HA_META",
  metricConfig: "METRIC",
};

export const CommonConfig: React.FC<IProps> = ({
  prefixName,
  values = {} as ICategory,
  form,
  tipsInfo,
}) => {
  const [state, setState] = useImmer({
    list: [],
    // 选项的下拉列表
    configList: [],
    originConfigList: [],
    configSelectedList:
      form.getFieldsValue()[prefixName]?.config?.filter((d) => d.key) || [],
    customItemName: "",
    currentCategoryType: values.type,
  });

  const { t } = useTranslation();

  const inputRef = useRef<InputRef>(null);

  const getPluginCategoriesValue = async (value: string) => {
    const result = await getPluginCategoriesByType(value);
    if (result.code === "SUCCESS") {
      const currentList = result.data.map((d) => {
        return {
          value: d,
          label: d,
        };
      });
      setState((draft) => {
        draft.list = currentList;
      });

      // 如果没有默认值，则将第一个设为默认值
      const originFormValue = form.getFieldsValue();

      const defaultFomrValue = originFormValue[prefixName] || {};

      if (!defaultFomrValue.type) {
        form.setFieldsValue({
          [prefixName]: {
            type: result.data[0],
          },
        });

        setState((draft) => {
          draft.currentCategoryType = result.data[0];
        });

        // 查询 config list
        await getPluginConfigList(value, result.data[0]);
      }
    }
  };

  const getPluginConfigList = async (category: string, type: string) => {
    const result = await getPluginCategoriesConfig(category, type);
    if (result.code === "SUCCESS") {
      const currentConfigList = result.data.map((d) => {
        return {
          label: d.comment,
          value: d.key,
          required: d.required,
          type: d.type,
          defaultValue: d.defaultValue,
          masked: d.masked,
        };
      });

      // 过滤出 required=true 的选项
      const requiredList = currentConfigList.filter((d) => d.required);
      const allListKeys = currentConfigList.map((d) => d.value);

      const originFormValue = form.getFieldsValue();
      const originDefaultConfig =
        originFormValue[prefixName]?.config?.filter((d) => d.key) || [];
      for (const rd of requiredList) {
        const { value, required, masked, defaultValue, type } = rd;
        // 是否已经存在yu originDefaultConfig 中
        const flag = originDefaultConfig.find((d) => d.key === rd.value);

        if (!flag) {
          originDefaultConfig.push({
            key: value,
            value: defaultValue,
            required,
            masked,
            type,
          });
        } else {
          if (!("masked" in flag)) {
            flag.masked = rd.masked;
          }
          if (!("type" in flag)) {
            flag.type = rd.type;
          }
          if (!("required" in flag)) {
            flag.required = rd.required;
          }
        }
      }
      // 将默认值和 required 的值进行合并

      // form.setFieldValue(prefixName, {
      //   ...originFormValue[prefixName],
      //   config: originDefaultConfig
      // })

      if (originFormValue[prefixName]) {
        form.setFieldsValue({
          [prefixName]: {
            ...originFormValue[prefixName],
            config: originDefaultConfig,
          },
        });
      }

      setState((draft) => {
        draft.originConfigList = currentConfigList;
        draft.configList = currentConfigList;
        draft.configSelectedList = originDefaultConfig;
      });
    }
  };

  useEffect(() => {
    if (DEFAULT_CATEGORY[prefixName]) {
      getPluginCategoriesValue(DEFAULT_CATEGORY[prefixName]);
    }
  }, [DEFAULT_CATEGORY[prefixName]]);

  useEffect(() => {
    if (
      DEFAULT_CATEGORY[prefixName] &&
      (values?.type || state.currentCategoryType)
    ) {
      getPluginConfigList(
        DEFAULT_CATEGORY[prefixName],
        values?.type || state.currentCategoryType
      );
    }
  }, [DEFAULT_CATEGORY[prefixName], values?.type]);

  const handleChangeType = (value: string) => {
    getPluginConfigList(DEFAULT_CATEGORY[prefixName], value);
  };

  const handleFocusConfig = () => {
    const { config } = form.getFieldsValue()[prefixName];
    const selectedKeys = config.map((d) => d.key);
    const otherItems = state.originConfigList.filter(
      (d) => !selectedKeys.includes(d.value)
    );
    setState((draft) => {
      draft.configList = otherItems;
    });
  };

  const handleChangeConfig = (value: string, index) => {
    const currentConfig = state.configList.find((d) => d.value === value);
    const { masked, defaultValue, type } = currentConfig;

    // 更新 input 值
    const originFormValue = form.getFieldsValue();
    originFormValue[prefixName].config[index] = {
      key: value,
      value: defaultValue,
      masked,
      type,
    };

    setState((draft) => {
      draft.configSelectedList = [
        ...state.configSelectedList,
        {
          key: value,
          value: defaultValue,
          masked,
          type,
        },
      ];

      draft.currentCategoryType = value;
    });

    // form.setFieldsValue({
    //   [prefixName]: {
    //     ...originFormValue[prefixName],
    //     config: [
    //       ...originDefaultConfig,
    //       {
    //         key: value,
    //         value: defaultValue,
    //         masked,
    //         type,
    //       },
    //     ],
    //   },
    // })
  };

  const onNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setState((draft) => {
      draft.customItemName = event.target.value;
    });
  };

  const addItem = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    // 验证添加的key是否重复
    const hasItem = state.originConfigList.find(
      (d) => d.value === state.customItemName
    );
    if (hasItem) {
      message.error(t("i18n.key.duplicate.keys"));
      return;
    }
    setState((draft) => {
      draft.configList = [
        ...state.configList,
        {
          label: state.customItemName,
          value: state.customItemName,
        },
      ];
      draft.customItemName = "";
    });
    setTimeout(() => {
      inputRef.current?.focus();
    }, 0);
  };

  const handleRemoveItem = (name: number, remove) => {
    remove(name);

    const originFormValue = form.getFieldsValue();
    const currentList =
      originFormValue[prefixName]?.config?.filter((d) => d.key) || [];

    const selectedKeys = originFormValue[prefixName]?.config?.map((d) => d.key);
    const otherItems = state.originConfigList.filter(
      (d) => !selectedKeys.includes(d.value)
    );

    setState((draft) => {
      draft.configSelectedList = currentList;
      draft.configList = otherItems;
    });
  };

  const handleAddItem = (add) => {
    const { config } = form.getFieldsValue()[prefixName];
    const selectedKeys = config.map((d) => d.key);
    const otherItems = state.originConfigList.filter(
      (d) => !selectedKeys.includes(d.value)
    );
    add({ key: null, value: null });
    setState((draft) => {
      draft.configList = otherItems;
    });
  };

  // 是否执行过一键安装
  const hasExecQuickInstall = localStorage.getItem("HAS_EXEC_QUICK_INSTALL");

  return (
    <>
      {!["runtimeMetaConfig", "haMetaConfig", "metricConfig"].includes(
        prefixName
      ) && (
        <Alert
          message={tipsInfo}
          type="info"
          showIcon
          style={{ marginBottom: 24 }}
        />
      )}
      <Form.Item name={[prefixName, "type"]} label={t("i18n.key.type")}>
        <Select
          style={{ width: 450 }}
          onChange={handleChangeType}
          disabled={!!hasExecQuickInstall}
        >
          {state.list.map((d) => {
            return <Select.Option value={d.value}>{d.label}</Select.Option>;
          })}
        </Select>
      </Form.Item>
      <Form.Item label={t("Configuration")}>
        <Form.List name={[prefixName, "config"]}>
          {(fields, { add, remove }) => (
            <>
              {fields.map(({ key, name, ...restField }, index) => (
                <Space
                  key={key}
                  style={{ display: "flex", marginBottom: 8 }}
                  align="baseline"
                >
                  <Form.Item
                    {...restField}
                    name={[name, "key"]}
                    rules={[
                      {
                        required: true,
                        message: t("i18n.key.parameter.required"),
                      },
                    ]}
                  >
                    <Select
                      style={{ width: 450 }}
                      onChange={(value) => {
                        handleChangeConfig(value, index);
                      }}
                      onFocus={handleFocusConfig}
                      disabled={!!hasExecQuickInstall}
                      dropdownRender={(menu) => (
                        <>
                          {menu}
                          <Divider style={{ margin: "8px 0" }} />
                          <Space style={{ padding: "0 8px 4px" }}>
                            <Input
                              placeholder={t("i18n.key.enter.key")}
                              ref={inputRef}
                              value={state.customItemName}
                              onChange={onNameChange}
                            />
                            <Button
                              type="text"
                              icon={<PlusOutlined />}
                              onClick={addItem}
                            >
                              {t("i18n.key.custom")}
                            </Button>
                          </Space>
                        </>
                      )}
                    >
                      {state.configList.map((d) => {
                        return (
                          <Select.Option value={d.value}>
                            {d.label}
                            <Tag
                              style={{ marginLeft: 8 }}
                              color={d.required ? "green" : "default"}
                            >
                              {d.value}
                            </Tag>
                          </Select.Option>
                        );
                      })}
                    </Select>
                  </Form.Item>
                  <Form.Item
                    {...restField}
                    name={[name, "value"]}
                    rules={[
                      {
                        required:
                          form.getFieldsValue()[prefixName]?.config[index]
                            .required,
                        message: t("i18n.key.parameter.value"),
                      },
                    ]}
                  >
                    {form.getFieldsValue()[prefixName]?.config[index].masked ? (
                      <Input.Password
                        placeholder="input password"
                        style={{ width: 350 }}
                        disabled={!!hasExecQuickInstall}
                      />
                    ) : (
                      <Input
                        placeholder={t("i18n.key.attribute.value")}
                        style={{ width: 350 }}
                        disabled={!!hasExecQuickInstall}
                      />
                    )}
                  </Form.Item>
                  {!form.getFieldsValue()[prefixName]?.config[index].required &&
                    !hasExecQuickInstall && (
                      <MinusCircleOutlined
                        onClick={() => handleRemoveItem(name, remove)}
                      />
                    )}
                </Space>
              ))}
              {!hasExecQuickInstall && (
                <Form.Item style={{ width: 810 }}>
                  <Button
                    type="dashed"
                    onClick={() => handleAddItem(add)}
                    block
                    icon={<PlusOutlined />}
                  >
                    {t("i18n.key.add.config")}
                  </Button>
                </Form.Item>
              )}
            </>
          )}
        </Form.List>
      </Form.Item>
    </>
  );
};
