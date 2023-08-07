import React from "react";
import { useHistory } from "umi";
import {
  EyeOutlined,
  EyeInvisibleOutlined,
  GlobalOutlined,
} from "@ant-design/icons";
import { Button, Form, Input, message, Menu } from "antd";
import { useTranslation } from "react-i18next";
import { PUBLIC_PERFIX_CLASS } from "../constants";
import { useAuth } from "../hooks/useAuth";
import styles from "./index.module.less";
import i18n from "../../../../../../i18n";

const { Item, useForm } = Form;

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaflowRegister = (props: PluginPorps) => {
  const redirectUrl = props?.redirectPath?.[0]?.path || "/";
  const [form] = useForm();
  const { onRegister, registerLoading } = useAuth();
  const history = useHistory();
  const { t } = useTranslation();

  const register = async () => {
    const values = await form.validateFields();
    const { name, password, comment } = values;
    if (values) {
      try {
        onRegister({ name, password, comment }).then((res) => {
          // 后端接口添加 success 字段的话就可以替换成 res.success
          if (res.code === "SUCCESS") {
            message.success(t("i18n.key.registration.succeeded"));
            window.location.href = redirectUrl;
          } else {
            message.error(t("i18n.key.registration.failed") + res.message);
          }
        });
      } catch (error) {
        message.error(error ?? t("i18n.key.registration.failed"));
      }
    }
  };

  const handleChangeLanguage = (value: string) => {
    localStorage.setItem("i18nextLng", value);
    i18n.change(value);
    location.reload();
  };

  return (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-container`]}>
      <Menu
        style={{
          position: "absolute",
          right: 8,
          top: 8,
          zIndex: 3,
          width: 200,
        }}
        mode="inline"
      >
        <Menu.SubMenu
          title={
            <>
              <GlobalOutlined style={{ marginRight: 8 }} />
              {t("i18n.key.switch.language")}
            </>
          }
        >
          <Menu.Item onClick={() => handleChangeLanguage("zh-CN")}>
            {t("i18n.key.chinese")}
          </Menu.Item>
          <Menu.Item onClick={() => handleChangeLanguage("en-US")}>
            {t("i18n.key.English")}
          </Menu.Item>
        </Menu.SubMenu>
      </Menu>

      <img
        src="https://mdn.alipayobjects.com/huamei_qcdryc/afts/img/A*AbamQ5lxv0IAAAAAAAAAAAAADgOBAQ/original"
        alt="geaflow-logo"
        className={styles[`${PUBLIC_PERFIX_CLASS}-logo-img`]}
      />
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-particles-container`]}>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-text`]}>
          {t("i18n.key.geaflow.title") === "Streaming Graph Computing" ? (
            <div className={styles[`${PUBLIC_PERFIX_CLASS}-title`]}>
              <span>{t("i18n.key.geaflow.title")}</span>
              <span style={{ fontSize: 40, color: "#69c0ff" }}>
                {t("i18n.key.geaflow.subtitle")}
              </span>
            </div>
          ) : (
            <div>
              {t("i18n.key.geaflow.title")}
              <br />
              {t("i18n.key.geaflow.subtitle")}
            </div>
          )}
        </div>
      </div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-form`]}>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-logo`]}>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-account-login`]}>
            {t("i18n.key.to.register")}
          </div>
          <Form
            form={form}
            className={styles[`${PUBLIC_PERFIX_CLASS}-form-style`]}
          >
            <Item
              name="name"
              rules={[
                {
                  required: true,
                  message: `${t("i18n.key.your.username")}`,
                },
              ]}
            >
              <Input placeholder={t("i18n.key.account")} />
            </Item>
            <Item name="comment">
              <Input placeholder={t("i18n.key.nickName")} />
            </Item>
            <Item
              name="password"
              rules={[
                {
                  required: true,
                  message: `${t("i18n.key.your.password")}`,
                },
              ]}
            >
              <Input.Password
                placeholder={t("i18n.key.password")}
                iconRender={(visible) =>
                  visible ? <EyeOutlined /> : <EyeInvisibleOutlined />
                }
              />
            </Item>
            <Item
              name="repassword"
              dependencies={["password"]}
              rules={[
                {
                  required: true,
                  message: `${t("i18n.key.confirmation.password")}`,
                },
                ({ getFieldValue }) => ({
                  validator(rule, value) {
                    if (!value || getFieldValue("password") === value) {
                      return Promise.resolve();
                    }
                    return Promise.reject(t("i18n.key.password.different"));
                  },
                }),
              ]}
            >
              <Input.Password
                placeholder={t("i18n.key.confirm.password")}
                iconRender={(visible) =>
                  visible ? <EyeOutlined /> : <EyeInvisibleOutlined />
                }
              />
            </Item>
            <Button
              type="primary"
              loading={registerLoading}
              onClick={() => register()}
            >
              {t("i18n.key.register")}
            </Button>
            <p style={{ marginTop: 8 }}>
              <a onClick={() => history.replace(redirectUrl)}>
                {t("i18n.key.already.account")}
              </a>
            </p>
          </Form>
        </div>
      </div>
    </div>
  );
};
