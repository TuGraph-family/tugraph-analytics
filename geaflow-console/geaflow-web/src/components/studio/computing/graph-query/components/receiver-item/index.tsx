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

import { Avatar } from "antd";
import MessageView from "../MessageView";
import { RobotOutlined } from "@ant-design/icons";
import styles from "./index.less";

const avatarStyle = { backgroundColor: "#005EFF", verticalAlign: "middle" };

const ReceiverItem = ({ userName, item, onCopy, onSend }) => {
  return (
    <div className={styles["receiver-item"]}>
      {/* receiver */}
      <div className={styles["avatar-wrap"]}>
        <Avatar size="large" style={avatarStyle}>
          <RobotOutlined style={{ fontSize: 20 }} />
        </Avatar>
      </div>
      <MessageView isSender={false} {...item} onCopy={onCopy} onSend={onSend} />
    </div>
  );
};

export default ReceiverItem;
