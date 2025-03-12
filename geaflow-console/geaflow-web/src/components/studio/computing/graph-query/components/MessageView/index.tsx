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

import cls from "classnames";
import moment from "moment";
import styles from "./index.less";
import $i18n from "@/components/i18n";
import React from "react";
import { Popover, Tooltip, message } from "antd";
import { CopyOutlined, RedoOutlined } from "@ant-design/icons";

const MessageView = ({
  onSend,
  onCopy,
  prompt,
  answer,
  isSender,
  createTime,
  status,
  ...rest
}) => {
  const textStyles = cls(styles["text-message-view"], {
    [styles.sender]: isSender,
    [styles.receiver]: !isSender,
  });

  const popMenuStyles = cls(styles["show-popmenu"], {
    [styles.sender]: isSender,
    [styles.receiver]: !isSender,
  });

  const bubbleStyles = cls(styles.bubble, {
    [styles.sender]: isSender,
    [styles.receiver]: !isSender,
  });

  const messageTimeStyle = cls(styles["message-time"], {
    [styles.sender]: isSender,
    [styles.receiver]: !isSender,
  });

  const renderPopuMenu = () => {
    return (
      <div className={popMenuStyles}>
        <CopyOutlined
          onClick={() => {
            onCopy?.(answer);
            message.info(
              `${$i18n.get({
                id: "openpiece-geaflow.job-detail.components.query.copy",
                dm: "已复制到编辑框",
              })}`
            );
          }}
        />
      </div>
    );
  };

  const renderMessageView = () => {
    // 文本消息
    if (isSender) {
      return <div className={styles["text-item"]}>{prompt}</div>;
    }

    // 图片消息
    if (!isSender) {
      return (
        <>
          {status === "FAILED" ? (
            <Tooltip
              trigger="click"
              title={answer}
              placement="left"
              overlayInnerStyle={{
                height: 500,
                overflow: "auto",
              }}
            >
              <div className={styles["image-item"]}>
                <div className={styles["text-item"]} style={{ color: "red" }}>
                  {$i18n.get({
                    id: "openpiece-geaflow.job-detail.components.query.error",
                    dm: "运行失败, 点击可查看详情",
                  })}
                </div>
              </div>
            </Tooltip>
          ) : (
            <div className={styles["image-item"]}>
              <div className={styles["text-item"]}>{answer}</div>
            </div>
          )}
        </>
      );
    }

    return null;
  };
  return (
    <div className={textStyles}>
      <div className={styles["text-message-wrapper"]}>
        <div className={messageTimeStyle}>{createTime}</div>
        <div className={bubbleStyles}>
          {renderMessageView()}
          {!isSender && status !== "FAILED" && renderPopuMenu()}
          {!isSender && status === "FAILED" && (
            <div className={popMenuStyles}>
              <RedoOutlined
                style={{ cursor: "pointer" }}
                onClick={() => {
                  onSend?.(prompt);
                }}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default MessageView;
