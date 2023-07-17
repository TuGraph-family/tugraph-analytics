import React, { useEffect, useState } from "react";
import { Tooltip, Input, Space, Table } from "antd";
import { getRemoteFiles, getRemoteFileId } from "../services/file-manage";
import { AddTemplateModal } from "./uploadModal";
import styles from "./list.module.less";
import $i18n from "../../../../../../i18n";

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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.jar-file-manage.FileName",
        dm: "文件名称",
      }),
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
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.jar-file-manage.Operator",
        dm: "操作人",
      }),
      key: "creatorName",
      width: 150,
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.jar-file-manage.Creator",
            dm: "创建人：",
          })}
          {record.creatorName} <br />
          {record?.modifierName && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.jar-file-manage.ModifiedByRecordmodifiername",
                  dm: "修改人：{recordModifierName}",
                },
                { recordModifierName: record.modifierName }
              )}
            </span>
          )}
        </span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.jar-file-manage.OperationTime",
        dm: "操作时间",
      }),
      key: "createTime",
      width: 250,
      render: (_, record: any) => (
        <span>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.jar-file-manage.CreationTime",
            dm: "创建时间：",
          })}
          {record.createTime} <br />
          {record?.modifyTime && (
            <span>
              {$i18n.get(
                {
                  id: "openpiece-geaflow.geaflow.jar-file-manage.ModificationTimeRecordmodifytime",
                  dm: "修改时间：{recordModifyTime}",
                },
                { recordModifyTime: record.modifyTime }
              )}
            </span>
          )}
        </span>
      ),
    },
    {
      title: $i18n.get({
        id: "openpiece-geaflow.geaflow.jar-file-manage.Operation",
        dm: "操作",
      }),
      key: "action",
      width: 130,
      render: (_, record) => (
        <Space>
          <a
            onClick={() => {
              getRemoteFileId(record?.id);
            }}
          >
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.jar-file-manage.Download",
              dm: "下载",
            })}
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
            {$i18n.get({
              id: "openpiece-geaflow.geaflow.jar-file-manage.Upload",
              dm: "上传",
            })}
          </a>
        </Space>
      ),
    },
  ];

  return (
    <div className={styles["file-manage"]}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <div style={{ fontWeight: 500, fontSize: 16 }}>
          {$i18n.get({
            id: "openpiece-geaflow.geaflow.jar-file-manage.FilesList",
            dm: "文件列表",
          })}
        </div>
        <div>
          <Search
            style={{ width: 286, marginRight: 16 }}
            placeholder={$i18n.get({
              id: "openpiece-geaflow.geaflow.jar-file-manage.EnterASearchKeyword",
              dm: "请输入搜索关键词",
            })}
            onSearch={(value) => {
              setTemplateList({ ...temeplateList, search: value });
            }}
          />
        </div>
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
