import React, { useCallback } from "react";
import { useHistory } from 'umi'
import { EyeOutlined, EyeInvisibleOutlined } from "@ant-design/icons";
import { Button, Form, Input, message } from "antd";
import { PUBLIC_PERFIX_CLASS } from "../constants";
import { useAuth } from "../hooks/useAuth";
import styles from "./index.module.less";

const { Item, useForm } = Form;

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaflowRegister = (props: PluginPorps) => {
  const redirectUrl = props?.redirectPath?.[0]?.path || '/';
  const [form] = useForm();
  const { onRegister, registerLoading } = useAuth();
  const history = useHistory()

  const register = async () => {
    const values = await form.validateFields();
    const { name, password, comment } = values;
    if (values) {
      try {
        onRegister({ name, password, comment }).then((res) => {
          // 后端接口添加 success 字段的话就可以替换成 res.success
          if (res.code === "SUCCESS") {
            message.success("注册成功！");
            window.location.href = redirectUrl;
          } else {
            message.error("注册失败！" + res.message);
          }
        });
      } catch (error) {
        message.error(error ?? "注册失败！");
      }
    }
  };
  return (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-container`]}>
      <img
        src="https://mdn.alipayobjects.com/huamei_qcdryc/afts/img/A*AbamQ5lxv0IAAAAAAAAAAAAADgOBAQ/original"
        alt="geaflow-logo"
        className={styles[`${PUBLIC_PERFIX_CLASS}-logo-img`]}
      />
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-particles-container`]}>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-text`]}>
          蚂蚁集团开源<br />实时图计算引擎
        </div>
      </div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-form`]}>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-logo`]}>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-account-login`]}>
            欢迎注册
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
                  message: "请输入账号!",
                },
              ]}
            >
              <Input placeholder="账号" />
            </Item>
            <Item name="comment">
              <Input placeholder="昵称" />
            </Item>
            <Item
              name="password"
              rules={[
                {
                  required: true,
                  message: "请输入密码！",
                },
              ]}
            >
              <Input.Password
                placeholder="密码"
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
                  message: "请输入确认密码！",
                },
                ({ getFieldValue }) => ({
                  validator(rule, value) {
                    if (!value || getFieldValue("password") === value) {
                      return Promise.resolve();
                    }
                    return Promise.reject("新密码与确认新密码不同！");
                  },
                }),
              ]}
            >
              <Input.Password
                placeholder="确认密码"
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
              注册
            </Button>
            <p style={{ marginTop: 8 }}>
              <a onClick={() => history.replace(redirectUrl)}>已有账号登录</a>
            </p>
          </Form>
        </div>
      </div>
    </div>
  );
};
