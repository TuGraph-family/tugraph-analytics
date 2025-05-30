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

import React from "react";
import {
  EyeOutlined,
  EyeInvisibleOutlined,
  GlobalOutlined,
} from "@ant-design/icons";
import { history } from "umi";
import { Button, Form, Input, message, Checkbox, Modal, Menu } from "antd";
import { useTranslation } from "react-i18next";
import { PUBLIC_PERFIX_CLASS } from "../../constants";
import { useAuth } from "../hooks/useAuth";
import { setLocalData } from "../util";
import { hasQuickInstall } from "../services/quickInstall";
import i18n from "@/components/i18n";
import styles from "./index.module.less";

const { Item, useForm } = Form;

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaflowLogin: React.FC<PluginPorps> = ({ redirectPath = [] }) => {
  const registerURL = () => {
    history.push("/register");
  };
  const [form] = useForm();
  const { onLogin, loginLoading } = useAuth();
  const { t } = useTranslation();

  const login = async () => {
    const values = await form.validateFields();

    if (values) {
      try {
        onLogin(values).then((res) => {
          // 后端接口添加 success 字段的话就可以替换成 res.success
          if (res.code === "SUCCESS") {
            const { sessionToken, systemSession } = res.data;

            document.cookie = `geaflow-token=${sessionToken};`;
            setLocalData("GEAFLOW_TOKEN", sessionToken);
            setLocalData("GEAFLOW_LOGIN_USERNAME", values.username);

            // 登录成功后就删除上一次缓存的实例
            localStorage.removeItem("GEAFLOW_CURRENT_INSTANCE");

            // 登录成功后，检查是否执行过一键安装
            hasQuickInstall(sessionToken).then((resp) => {
              // 管理员且没有安装
              if (systemSession) {
                message.success(t("i18n.key.login.succeeded"));
                // 管理员登录，设置登录用户角色
                localStorage.setItem("IS_GEAFLOW_ADMIN", "true");
                // 没有安装
                if (!resp.data || resp.data === "false") {
                  // 没有执行过一键安装操作，删除可能的缓存值
                  localStorage.removeItem("HAS_EXEC_QUICK_INSTALL");
                  // 跳转到一键安装页面
                  history.push("/quickInstall");
                  location.reload();
                } else {
                  localStorage.setItem("HAS_EXEC_QUICK_INSTALL", "true");
                  localStorage.removeItem("QUICK_INSTALL_PARAMS");
                  history.push("/system/ColClusterManage");
                  location.reload();
                }
              } else {
                localStorage.removeItem("IS_GEAFLOW_ADMIN");
                // 非管理员
                if (!resp.data) {
                  // 没有安装，提示联系管理员安装，跳转到主页面
                  Modal.warning({
                    title: t("i18n.key.not.initialized"),
                    content: t("i18n.key.installation"),
                  });
                } else {
                  localStorage.setItem("HAS_EXEC_QUICK_INSTALL", "true");
                  message.success(t("i18n.key.login.succeeded"));
                  history.push("/studio");
                  location.reload();
                }
              }
            });
          } else {
            message.error(t("i18n.key.login.failed") + res.message);
          }
        });
      } catch (error) {
        message.error(error ?? t("i18n.key.login.failed"));
      }
    }
  };

  const toRegistryPage = () => {
    registerURL();
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
          {/* <img
             src="https://mdn.alipayobjects.com/huamei_qcdryc/afts/img/A*ASz1S5q2zRYAAAAAAAAAAAAADgOBAQ/original"
             alt="tugraph-slogan"
            ></img> */}
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
            {t("i18n.key.Welcome.login")}
          </div>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-desc`]}>
            {t("i18n.key.userName.password")}
          </div>
          <Form
            form={form}
            className={styles[`${PUBLIC_PERFIX_CLASS}-form-style`]}
          >
            <Item
              name="username"
              rules={[
                {
                  required: true,
                  message: `${t("i18n.key.your.username")}`,
                },
              ]}
            >
              <Input placeholder={t("i18n.key.account")} />
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
            <Item name="systemLogin" valuePropName="checked">
              <Checkbox>{t("i18n.key.administrator.login")}</Checkbox>
            </Item>
            <Button
              type="primary"
              loading={loginLoading}
              onClick={() => login()}
            >
              {t("i18n.key.login")}
            </Button>
            <p style={{ marginTop: 8 }}>
              <a onClick={toRegistryPage}>{t("i18n.key.new.user")}</a>
            </p>
          </Form>
        </div>
      </div>
    </div>
  );
};
