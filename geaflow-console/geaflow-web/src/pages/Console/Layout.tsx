import { Outlet } from "umi";
import { Layout } from "antd";
import { MENU_List_MAP } from "./Menu";
import { NavigationMenu } from "@/layouts/components/NavigationMenu";
import React from "react";
import styles from "./Layout.module.less";
import "../../i18n";
const { Content } = Layout;

const LayoutPage: React.FC = () => {
  return (
    <Layout className={styles["layout-content-container"]}>
      <NavigationMenu
        data={MENU_List_MAP}
        defaultActive={MENU_List_MAP[0].key}
        defaultSelect={MENU_List_MAP[0].children[0].key}
      />
      <Layout className={styles["layout-content"]}>
        <Content className={styles["layout-body"]}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
};
export default LayoutPage;
