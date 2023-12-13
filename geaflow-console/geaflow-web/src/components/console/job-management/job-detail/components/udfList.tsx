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
