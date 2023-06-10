import React, { useCallback } from 'react';
import { EyeOutlined, EyeInvisibleOutlined } from '@ant-design/icons';
import { useHistory } from 'umi'
import { useOpenpieceUserAuth } from '@tugraph/openpiece-client' 
import { Button, Form, Input, message, Checkbox, Modal } from 'antd';
import { PUBLIC_PERFIX_CLASS } from '../constants';
import { useAuth } from '../hooks/useAuth';
import { setLocalData } from '../util';
import { hasQuickInstall } from '../services/quickInstall';
import styles from './index.module.less';

const { Item, useForm } = Form;

interface PluginPorps {
  redirectPath?: RedirectPath[];
}

interface RedirectPath {
  path: string;
  pathName: string;
}

export const GeaflowLogin: React.FC<PluginPorps> = ({ redirectPath = [] }) => {
  const registerURL = redirectPath.find(d => d.pathName === '注册页面')

  const [form] = useForm();
  const { onLogin, loginLoading } = useAuth();
  const { switchRole } = useOpenpieceUserAuth()
  const history = useHistory()

  const login = async () => {
    const values = await form.validateFields();
    
    if (values) {
      try {
        onLogin(values).then((res) => {
          // 后端接口添加 success 字段的话就可以替换成 res.success
          if (res.code === 'SUCCESS') {
            const { sessionToken, systemSession } = res.data
            
            document.cookie = `geaflow-token=${sessionToken};`;
            setLocalData('GEAFLOW_TOKEN', sessionToken);
            setLocalData('GEAFLOW_LOGIN_USERNAME', values.username)

            // 登录成功后就删除上一次缓存的实例
            localStorage.removeItem('GEAFLOW_CURRENT_INSTANCE')

            // 登录成功后，检查是否执行过一键安装
            hasQuickInstall().then(resp => {
              
              // 管理员且没有安装
              if (systemSession) {
                message.success('登录成功！');
                // 管理员登录，设置登录用户角色
                localStorage.setItem('IS_GEAFLOW_ADMIN', 'true')
                // 没有安装
                if (!resp.data || resp.data === 'false') {
                  // 没有执行过一键安装操作，删除可能的缓存值
                  localStorage.removeItem('HAS_EXEC_QUICK_INSTALL')
                  // 跳转到一键安装页面
                  const installURL = redirectPath.find(d => d.pathName === '一键安装')
                  // window.location.href = installURL.path;
                  switchRole('admin', installURL?.path)
                } else {
                  localStorage.setItem('HAS_EXEC_QUICK_INSTALL', 'true')
                  localStorage.removeItem('QUICK_INSTALL_PARAMS')
                  const homeURL = redirectPath.find(d => d.pathName === '集群管理')
                  // window.location.href = homeURL.path;
                  switchRole('admin', homeURL?.path)
                }
              } else {
                localStorage.removeItem('IS_GEAFLOW_ADMIN')
                // 非管理员
                if (!resp.data) {
                  // 没有安装，提示联系管理员安装，跳转到主页面
                  Modal.warning({
                    title: 'GeaFlow系统尚未初始化',
                    content: `请联系管理员对GeaFlow初始化，或者勾选“管理员登录”登入系统开始一键安装。`,
                  });
                } else {
                  const homeURL = redirectPath.find(d => d.pathName === '图计算')
                  message.success('登录成功！');
                  switchRole('member', homeURL?.path)
                }
              }
            })           
          } else {
            message.error('登录失败！' + res.message);
          }
        });
      } catch (error) {
        message.error(error ?? '登录失败！');
      }
    }
  };

  const toRegistryPage = () => {
    history.replace(registerURL?.path)
  }

  return (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-container`]}>
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
          蚂蚁集团开源<br />实时图计算引擎
        </div>
      </div>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-form`]}>
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-logo`]}>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-account-login`]}>欢迎登录</div>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-login-desc`]}>请使用账号密码登录</div>
          <Form form={form} className={styles[`${PUBLIC_PERFIX_CLASS}-form-style`]}>
            <Item
              name="username"
              rules={[
                {
                  required: true,
                  message: '请输入用户名',
                },
              ]}
            >
              <Input placeholder="账号" />
            </Item>
            <Item
              name="password"
              rules={[
                {
                  required: true,
                  message: '请输入密码！',
                },
              ]}
            >
              <Input.Password
                placeholder="密码"
                iconRender={(visible) => (visible ? <EyeOutlined /> : <EyeInvisibleOutlined />)}
              />
            </Item>
            <Item name="systemLogin" valuePropName="checked">
              <Checkbox>管理员登录</Checkbox>
            </Item>
            <Button type="primary" loading={loginLoading} onClick={() => login()}>
              登录
            </Button>
            <p style={{ marginTop: 8 }}>
              <a onClick={toRegistryPage}>新用户注册</a>
            </p>
          </Form>
        </div>
      </div>
    </div>
  );
};
