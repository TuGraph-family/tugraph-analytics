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

import { Outlet } from "umi";
import { Layout } from "antd";
import { MENU_List_MAP } from "./Menu";
import { NavigationMenu } from "@/layouts/components/NavigationMenu";
import React from "react";
import styles from "./Layout.module.less";
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
