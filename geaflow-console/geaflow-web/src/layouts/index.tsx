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

import { Link, Outlet, useLocation } from "umi";
import Header from "./Header";
import React, { useEffect, useState } from "react";
import styles from "./index.less";
import "../i18n";

export default function Layout() {
  const [isStick, setIsStick] = useState<boolean>(false);
  const handleScroll = () => {
    if (Number(window.scrollY) > 0) {
      setIsStick(true);
    } else {
      setIsStick(false);
    }
  };
  useEffect(() => {
    window.addEventListener("scroll", handleScroll, true);
    return () => {
      window.removeEventListener("scroll", handleScroll, true);
    };
  }, []);

  const location = useLocation();
  let Container =
    ["/", "/login", "/register"].indexOf(
      location.pathname
    ) === -1;
  return (
    <div className={styles.navs}>
      {Container && <Header isStick={isStick} />}
      <div className={styles.containerWrapper}>
        <Outlet />
      </div>
    </div>
  );
}
