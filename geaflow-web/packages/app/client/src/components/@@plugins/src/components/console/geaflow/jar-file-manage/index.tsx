import React, { useEffect, useState } from "react";
import { Tooltip, Input, Space, Table } from "antd";
import { getRemoteFiles, getRemoteFileId } from "../services/file-manage";
import { AddTemplateModal } from "./uploadModal";
import styles from "./list.module.less";

const { Search } = Input;

export const GeaflowJarfileManage: React.FC<{}> = ({}) => {
  const [temeplateList, setTemplateList] = useState({
    search: "",
    manageData: [],
    copyData: [],
    isAddMd: false,
    id: "",
  });
  const handelTemplata = async () => {
    const manageData = await getRemoteFiles({ name: temeplateList.search });
    setTemplateList({ ...temeplateList, manageData, copyData: manageData });
  };

  useEffect(() => {
    handelTemplata();
  }, [temeplateList.search]);
  const columns = [
    {
      title: "文件名称",
      dataIndex: "name",
      key: "name",
      width: 150,
      ellipsis: {
        showTitle: false,
      },
      render: (_, record: any) => (
        <span>
          <span>{record.name || "-"}</span>

          <br />
          {record?.comment && (
            <Tooltip placement="topLeft" title={record.comment}>
              <span style={{ fontSize: 12, color: "#ccc" }}>
                {record.comment}
              </span>
            </Tooltip>
          )}
        </span>
      ),
    },

    {
      title: "MD5",
      dataIndex: "md5",
      key: "md5",
      width: 150,
      ellipsis: {
        showTitle: false,
      },
      render: (md5: string) => (
        <Tooltip placement="topLeft" title={md5}>
          {md5 || "-"}
        </Tooltip>
      ),
    },
    {
      title: "URL",
      dataIndex: "url",
      key: "url",
      width: 200,
      ellipsis: {
        showTitle: false,
      },
      render: (url: string) => (
        <Tooltip placement="topLeft" title={url}>
          {url || "-"}
        </Tooltip>
      ),
    },
    {
      title: "操作人",
      key: "creatorName",
      width: 150,
      render: (_, record: any) => (
        <span>
          创建人：{record.creatorName} <br />
          {record?.modifierName && <span>修改人：{record.modifierName}</span>}
        </span>
      ),
    },
    {
      title: "操作时间",
      key: "createTime",
      width: 300,
      render: (_, record: any) => (
        <span>
          创建时间：{record.createTime} <br />
          {record?.modifyTime && <span>修改时间：{record.modifyTime}</span>}
        </span>
      ),
    },
    {
      title: "操作",
      key: "action",
      width: 80,
      render: (_, record) => (
        <Space>
          <a
            onClick={() => {
              getRemoteFileId(record?.id);
            }}
          >
            下载
          </a>
          <a
            onClick={() => {
              setTemplateList({
                ...temeplateList,
                isAddMd: true,
                id: record.id,
              });
            }}
          >
            上传
          </a>
        </Space>
      ),
    },
  ];
  return (
    <div className={styles["file-manage"]}>
      <div className={styles["file-search"]}>
        <Search
          style={{ width: 286, marginRight: 16 }}
          placeholder="请输入搜索关键词"
          onSearch={(value) => {
            setTemplateList({ ...temeplateList, search: value });
          }}
        />
      </div>
      <Table
        dataSource={temeplateList.manageData}
        columns={columns}
        pagination={{
          hideOnSinglePage: true,
          showQuickJumper: true,
          size: "small",
        }}
      />
      {/* 新增 */}
      <AddTemplateModal
        isAddMd={temeplateList.isAddMd}
        id={temeplateList.id}
        setTemplateList={setTemplateList}
        temeplateList={temeplateList}
      />
    </div>
  );
};
