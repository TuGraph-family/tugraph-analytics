import {
  AppstoreAddOutlined,
  ArrowLeftOutlined,
  DownloadOutlined,
  QuestionCircleOutlined,
  SaveOutlined,
} from "@ant-design/icons";
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
  Tooltip,
  message,
} from "antd";
import { filter, find, isEmpty, join, map, uniqueId } from "lodash";
import { useCallback, useEffect } from "react";
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
} from "../../services/computing";

import styles from "./index.module.less";

const { Option } = Select;

export const GraphQuery = (props: any) => {
  const { instance } = props;
  useEffect(() => {}, [instance]);

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

  const graphList = getLocalData("TUGRAPH_SUBGRAPH_LIST");
  const [state, updateState] = useImmer<{
    activeTab: string;
    isListShow: boolean;
    currentGraphName: string;
    graphListOptions: { label: string; value: string }[];
    editorWidth: number | string;
    editorHeight: number;
    pathHeight: number;
    script: string;
    resultData: Array<any & { id?: string }>;
    queryList: Array<{
      id: string;
      value: string;
      script: string;
      isEdit?: boolean;
    }>;
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
    storedVisible: boolean;
    result: string;
    record?: any;
    loading: boolean;
    isOlaps: string;
  }>({
    graphListOptions: map(graphList, (graph: any) => {
      return {
        label: graph.graph_name,
        value: graph.graph_name,
      };
    }),
    activeTab: IQUIRE_LIST[0].key,
    isListShow: true,
    currentGraphName: "",
    editorWidth: 350,
    editorHeight: 372,
    pathHeight: 388,
    script: "match (a)-[e]->(b) return a,e,b limit 10",
    resultData: [],
    queryList: [],
    editorKey: "",
    graphData: { nodes: [], edges: [] },
    editor: {},
    storedVisible: false,
    result: "",
    record: {},
    loading: false,
    isOlaps: "",
  });
  const {
    activeTab,
    isListShow,
    currentGraphName,
    graphListOptions,
    editorWidth,
    editorHeight,
    pathHeight,
    script,
    resultData,
    queryList,
    editorKey,
    graphData,
    editor,
    storedVisible,
    record,
    loading,
    isOlaps,
  } = state;

  const onSplitPaneWidthChange = useCallback((size: number) => {
    updateState((draft) => {
      draft.editorWidth = size;
    });
  }, []);
  const onSplitPaneHeightChange = useCallback((size: number) => {
    updateState((draft) => {
      draft.editorHeight = size;
    });
  }, []);
  const onSplitPanePathHeightChange = useCallback((size: number) => {
    updateState((draft) => {
      draft.pathHeight = size;
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
          }, 2500);
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
          <div style={{ gap: "24px", display: "flex" }}>
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
          </div>
        </Space>
      </div>
    </div>
  );

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
        </div>
      </div>
    </div>
  );
};
