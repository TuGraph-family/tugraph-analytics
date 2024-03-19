import { SendOutlined, CommentOutlined } from "@ant-design/icons";
import GremlinEditor from "ace-gremlin-editor";
import $i18n from "@/components/i18n";
import {
  Button,
  Divider,
  Empty,
  Popover,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tabs,
  FloatButton,
  message,
  Input,
  Drawer,
  Tooltip,
} from "antd";
import { filter, find, isEmpty, join, map, uniqueId } from "lodash";
import { useCallback, useEffect, useState } from "react";
import { useImmer } from "use-immer";
import IconFont from "./icon-font";
import { SplitPane } from "./split-panle";
import {
  IQUIRE_LIST,
  PUBLIC_PERFIX_CLASS,
  STORED_PROCEDURE_DESC,
  STORED_PROCEDURE_RULE,
} from "../constant";
import ExcecuteResultPanle from "./components/excecute-result-panle";
import { NodeQuery } from "./components/node-query";
import { PathQueryPanel } from "./components/path-query";
import { StatementList } from "./components/statement-query-list";
import {
  getOlapsResult,
  deleteOlapQueryId,
  getOlapQueryId,
  getChat,
  postChat,
  DeleteChat,
  getllmsList,
  postCallSync,
} from "../../services/computing";
import ReceiverItem from "./components/receiver-item";
import SenderItem from "./components/sender-item";
import { CaretRightOutlined } from "@ant-design/icons";
import styles from "./index.module.less";
import moment from "moment";

const { Option } = Select;

export const GraphQuery = (props: any) => {
  const receiverName = "GPT";

  const { instance } = props;

  const getLocalData = (key: string) => {
    if (!key) {
      return;
    }
    try {
      const data = JSON.parse(localStorage.getItem(key) || "{}");
      return data;
    } catch (e) {
      console.error(`tugraph ${key} %d ${e}`);
    }
  };

  const [open, setOpen] = useState<boolean>(false);
  const [state, updateState] = useImmer<{
    currentGraphName: string;
    editorHeight: number;
    script: string;
    resultData: Array<any & { id?: string }>;
    editorKey: string;
    graphData?: {
      nodes: Array<{
        indexs: string;
        labelName: string;
        nodeType: string;
        primary: string;
        properties: Array<{ id: string; name: string }>;
      }>;
      edges: Array<{
        edgeType: string;
        indexs: string;
        labelName: string;
        edgeConstraints: Array<Array<string>>;
      }>;
    };
    editor: any;
    result: string;
    record?: any;
    loading: boolean;
    isOlaps: string;
    chatList: any;
    chatValue: string;
    llsList: any;
    modelId: string;
    withSchema: boolean;
  }>({
    currentGraphName: "",
    editorHeight: 372,
    script: "match (a)-[e]->(b) return a,e,b limit 10",
    resultData: [],
    editorKey: "",
    graphData: { nodes: [], edges: [] },
    editor: {},
    result: "",
    record: {},
    loading: false,
    isOlaps: "",
    chatList: [],
    chatValue: "",
    llsList: [],
    modelId: "",
    withSchema: true,
  });
  const {
    currentGraphName,
    editorHeight,
    script,
    resultData,
    editorKey,
    graphData,
    editor,
    record,
    loading,
    isOlaps,
    chatList,
    chatValue,
    llsList,
    modelId,
    withSchema,
  } = state;

  const handleChat = () => {
    getChat({
      jobId: instance.id,
    }).then((res) => {
      if (res.success) {
        updateState((draft) => {
          draft.chatList = res.data.list;
        });
      }
    });
  };

  useEffect(() => {
    handleChat();
    getllmsList().then((res) => {
      updateState((draft) => {
        draft.llsList = res;
        draft.modelId = res[0]?.id;
      });
    });
  }, [instance, open]);

  const onSplitPaneHeightChange = useCallback((size: number) => {
    updateState((draft) => {
      draft.editorHeight = size;
    });
  }, []);

  const handleQuery = (
    limit: number,
    conditions: Array<{ property: string; value: string; operator: string }>,
    queryParams: string
  ) => {
    updateState((draft) => {
      draft.loading = true;
    });
    if (script && instance.id) {
      getOlapsResult({ script: script, jobId: instance.id }).then((res) => {
        if (res) {
          const getOlapId = window.setInterval(() => {
            getOlapQueryId(res).then((res) => {
              updateState((draft) => {
                draft.resultData = [
                  { id: res.id, result: res.result, status: res.status },
                ];
                draft.loading = false;
                draft.isOlaps = `${res.id}-add`;
                draft.record = res;
              });
              window.clearInterval(getOlapId);
            });
          }, 2000);
        } else {
          updateState((draft) => {
            draft.loading = false;
          });
        }
      });
    }
  };

  const handleDelete = () => {
    if (!isEmpty(record)) {
      deleteOlapQueryId(record.id).then((res) => {
        if (res.success) {
          updateState((draft) => {
            draft.resultData = [];
            draft.record = {};
            draft.script = "match (a)-[e]->(b) return a,e,b limit 10";
            draft.isOlaps = `${record.id}-del`;
          });
          if (!isEmpty(editor)) {
            editor?.editorInstance?.setValue?.(
              "match (a)-[e]->(b) return a,e,b limit 10"
            );
          }
          message.success("删除成功");
        } else {
          message.error("删除失败");
        }
      });
    }
  };

  const actionBar = (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-right-bar`]}>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-left-btn`]}>
        <Space split={<Divider type="vertical" />}>
          <div style={{ gap: "24px", display: "flex", alignItems: "center" }}>
            <Select disabled defaultValue={"ISOGQL"}>
              <Option value="Cypher">Cypher</Option>
              <Option value="ISOGQL">ISOGQL</Option>
            </Select>
            <Button
              className={styles[`${PUBLIC_PERFIX_CLASS}-btn-implement`]}
              loading={loading}
              type="primary"
              onClick={handleQuery}
              icon={
                <IconFont
                  type="icon-zhihang"
                  className={
                    styles[`${PUBLIC_PERFIX_CLASS}-btn-implement-zhixing`]
                  }
                />
              }
            >
              {$i18n.get({
                id: "openpiece-geaflow.job-detail.components.query.Execute",
                dm: "执行",
              })}
            </Button>
            <img
              src="https://mdn.alipayobjects.com/huamei_0bwegv/afts/img/A*k1GoQ5rfeCcAAAAAAAAAAAAADu3UAQ/original"
              alt=""
              style={{ width: 32, height: 32, cursor: "pointer" }}
              onClick={() => {
                setOpen(true);
              }}
            />
          </div>
        </Space>
      </div>
    </div>
  );

  const onSendMessage = (answer?: string) => {
    updateState((draft) => {
      draft.chatValue = "";
    });
    if ((chatValue || answer) && modelId) {
      console.log(answer, "answer");
      const date = new Date();
      const tmpRecord = {
        prompt: chatValue,
        answer: `${$i18n.get({
          id: "openpiece-geaflow.job-detail.components.query.running",
          dm: "思考中,请稍等...",
        })}`,
        status: "RUNNING",
        createTime: moment(date).format("yyyy-MM-DD HH:mm:ss"),
      };
      const tmpList = [...chatList, tmpRecord];
      updateState((draft) => {
        draft.chatList = tmpList;
      });
      postCallSync({
        modelId,
        withSchema,
        jobId: instance.id,
        prompt: chatValue || answer,
        saveRecord: true,
      }).then((res) => {
        if (res.success) {
          // const getChatAnwser = window.setInterval(() => {
          handleChat();
          //   window.clearInterval(getChatAnwser);
          // }, 2500);
        }
      });
    }
  };
  const onClose = () => {
    setOpen(false);
  };

  return (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-container`]}>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-content`]}>
        <StatementList
          onSelect={(activeId, result, record, script) => {
            if (!isEmpty(editor)) {
              editor?.editorInstance?.setValue?.(script);
            }
            updateState((draft) => {
              draft.resultData = [
                { id: activeId, result, status: record?.status },
              ];
              draft.record = record;
              draft.script = script;
            });
          }}
          record={record}
          instance={instance}
          isOlaps={isOlaps}
        />
        <div className={styles[`${PUBLIC_PERFIX_CLASS}-content-right`]}>
          <div className={styles[`${PUBLIC_PERFIX_CLASS}-content-right-top`]}>
            {actionBar}
            <div
              style={{ height: "100%", position: "relative" }}
              className={styles[`${PUBLIC_PERFIX_CLASS}-split-pane`]}
            >
              <SplitPane
                split="horizontal"
                defaultSize={editorHeight}
                onChange={onSplitPaneHeightChange}
              >
                <div
                  className={join(
                    [
                      styles[`${PUBLIC_PERFIX_CLASS}-right-center`],
                      styles[`${PUBLIC_PERFIX_CLASS}-split-pane`],
                    ],
                    " "
                  )}
                >
                  <div
                    style={{
                      flex: 1,
                      height: "100%",
                      position: "absolute",
                      width: "100%",
                      marginTop: 20,
                    }}
                  >
                    <GremlinEditor
                      gremlinId="test"
                      initValue={script}
                      onInit={(initEditor) => {
                        updateState((draft) => {
                          draft.editor = initEditor;
                          if (editorKey) {
                            const value = find(
                              getLocalData("TUGRAPH_STATEMENT_LISTS")[
                                currentGraphName
                              ],
                              (item) => item.id === editorKey
                            )?.script;
                            initEditor?.editorInstance?.setValue?.(value);
                            draft.script = value;
                          }
                        });
                      }}
                      onValueChange={(val) => {
                        updateState((draft) => {
                          draft.script = val;
                        });
                      }}
                      height={372}
                    />
                  </div>
                </div>
                <div
                  className={
                    styles[`${PUBLIC_PERFIX_CLASS}-content-right-bottom`]
                  }
                >
                  {resultData.length ? (
                    <ExcecuteResultPanle
                      queryResultList={resultData}
                      graphData={graphData}
                      graphName={currentGraphName}
                      record={record}
                      handleDelete={handleDelete}
                    />
                  ) : (
                    <div
                      className={styles[`${PUBLIC_PERFIX_CLASS}-bottom-spin`]}
                    >
                      <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
                    </div>
                  )}
                </div>
              </SplitPane>
            </div>
          </div>
          <Drawer
            // title="Basic Drawer"
            placement="right"
            width={500}
            onClose={onClose}
            open={open}
            extra={
              <Space>
                <div className={styles["schema"]}>
                  <p className={styles["schema-label"]}>
                    {$i18n.get({
                      id: "openpiece-geaflow.job-detail.components.query.schema",
                      dm: "带schema",
                    })}
                  </p>

                  <Switch
                    checked={withSchema}
                    onChange={(checked: boolean) => {
                      updateState((draft) => {
                        draft.withSchema = checked;
                      });
                    }}
                  />
                </div>

                <Select
                  style={{ width: 200 }}
                  placeholder="请选择模型"
                  defaultValue={llsList[0]?.name}
                  onChange={(value) => {
                    updateState((draft) => {
                      draft.modelId = value;
                    });
                  }}
                >
                  {llsList?.map((item: { id: string; name: string }) => {
                    return (
                      <Select.Option value={item.id} key={item.id}>
                        {item.name}
                      </Select.Option>
                    );
                  })}
                </Select>
                <Button
                  onClick={() => {
                    DeleteChat(instance.id).then((res) => {
                      if (res.success) {
                        handleChat();
                      }
                    });
                  }}
                >
                  {$i18n.get({
                    id: "openpiece-geaflow.job-detail.components.query.Clear",
                    dm: "清空",
                  })}
                </Button>
              </Space>
            }
          >
            <div className={styles["chat-container"]}>
              <div id="chatItems" className={styles["chat-items"]}>
                {loading && (
                  <div className={styles["chat-loading"]}>
                    <Spin />
                  </div>
                )}
                {!isEmpty(chatList) &&
                  chatList?.map((item, index) => {
                    return (
                      <div>
                        <div className={styles["chat-item"]} key={index}>
                          <SenderItem
                            userName={item?.creatorName}
                            item={item}
                          />
                        </div>
                        <div className={styles["chat-item"]} key={index}>
                          <ReceiverItem
                            userName={receiverName}
                            item={item}
                            onCopy={(copyScript) => {
                              if (!isEmpty(editor)) {
                                editor?.editorInstance?.setValue?.(copyScript);
                              }
                              updateState((draft) => {
                                draft.script = copyScript;
                              });
                            }}
                            onSend={(answer) => {
                              onSendMessage(answer);
                            }}
                          />
                        </div>
                      </div>
                    );
                  })}
              </div>
              <div className={styles["chat-input-operator"]}>
                <Input.TextArea
                  className={styles["chat-input"]}
                  placeholder={$i18n.get({
                    id: "openpiece-geaflow.job-detail.components.query.info",
                    dm: "请输入消息",
                  })}
                  autoSize={{ minRows: 4, maxRows: 6 }}
                  value={chatValue}
                  onChange={(e) => {
                    updateState((draft) => {
                      draft.chatValue = e.target.value;
                    });
                  }}
                  onPressEnter={(event) => {
                    if (
                      event.key === "Enter" &&
                      !event.shiftKey &&
                      event.keyCode == 13
                    ) {
                      event.preventDefault();
                      onSendMessage();
                    }
                  }}
                  // 按下回车后的回调
                />

                <SendOutlined
                  className={styles["chat-button"]}
                  onClick={() => {
                    onSendMessage();
                  }}
                />
              </div>
            </div>
          </Drawer>
        </div>
      </div>
    </div>
  );
};
