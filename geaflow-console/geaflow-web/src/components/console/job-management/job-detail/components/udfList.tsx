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

import React, { useEffect, useState } from "react";
import { InboxOutlined } from "@ant-design/icons";
import { Table, message, Modal, Popconfirm, Upload, Form } from "antd";
import moment from "moment";

interface UDFListProps {
  record: any;
  stageType: string;
}

const UDFList: React.FC<UDFListProps> = ({ record, stageType }) => {
  const data = record?.release?.job?.jarPackage
    ? [record?.release?.job?.jarPackage]
    : [];
  const columns = [
    {
      title: "名称",
      dataIndex: "name",
      key: "name",
      // render: (text: string, record: any) => {
      //   return (
      //     <a
      //       onClick={() => {
      //         getJarDownLoad(record?.uploadUrl, record?.fileName);
      //         message.info("开始下载，请稍后！");
      //       }}
      //     >
      //       {text}
      //     </a>
      //   );
      // },
    },
    {
      title: "MD5",
      dataIndex: "md5",
      key: "md5",
    },
    {
      title: "修改时间",
      dataIndex: "modifyTime",
      key: "modifyTime",
    },
  ];

  return <Table columns={columns} dataSource={data} pagination={false} />;
};

export default UDFList;
