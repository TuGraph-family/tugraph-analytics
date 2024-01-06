import { GraphinContextType, Layout, Utils } from "@antv/graphin";
import {
  AutoComplete,
  Button,
  Drawer,
  Empty,
  Form,
  Input,
  InputNumber,
  Popconfirm,
  Radio,
  Select,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Spin,
} from "antd";
import copy from "copy-to-clipboard";
import JSONBig from "json-bigint";
import { cloneDeep, filter, find, isEmpty, map, omit, uniqBy } from "lodash";
import React, { useCallback, useEffect } from "react";
import ReactJSONView from "react-json-view";
import { useImmer } from "use-immer";
import {
  GraphCanvas,
  GraphCanvasContext,
  GraphCanvasContextInitValue,
} from "../garph-canvas";
import { GraphCanvasLayout } from "../graph-canvas-layout";
import { GraphCanvasTools } from "../graph-canvas-tools";
import IconFont from "../icon-font";
import IconItem from "../icon-item";
import SwitchDrawer from "../switch-drawer";
import { DeleteOutlined } from "@ant-design/icons";
import { PROPERTY_TYPE, PUBLIC_PERFIX_CLASS } from "../../../constant";
import { useVisible } from "../hooks/useVisible";
import { useGraphData } from "../hooks/useGraphData";
import { editEdgeParamsTransform } from "../../utils/editEdgeParamsTransform";
import styles from "./index.module.less";
import $i18n from "@/components/i18n";

const { TabPane } = Tabs;
const { Group } = Radio;

interface ResultProps {
  excecuteResult?: any;
  graphData?: any;
  graphName?: string;
  modalOpen?: boolean;
  onClose?: () => void;
  record?: any;
  handleDelete: () => void;
  queryId?: string;
}

const ExecuteResult: React.FC<ResultProps> = ({
  excecuteResult,
  graphData,
  graphName,
  modalOpen,
  onClose: onCancel,
  record,
  handleDelete,
  queryId,
}) => {
  const {
    onCreateEdge,
    onCreateNode,
    onDeleteEdge,
    onDeleteNode,
    onEditEdge,
    onEditNode,
    EditEdgeLoading,
    EditNodeLoading,
    CreateEdgeLoading,
    CreateNodeLoading,
  } = useGraphData();
  // const viewResult = {
  //   edges: [
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Morpheus",
  //       },
  //       // id: "0_0_0_3937_0",
  //       source: "0",
  //       target: "3937",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Morpheus",
  //       },
  //       // id: "0_0_0_3938_0",
  //       source: "0",
  //       target: "3938",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Morpheus",
  //       },
  //       // id: "0_0_0_3939_0",
  //       source: "0",
  //       target: "3939",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Trinity",
  //       },
  //       // id: "1_0_0_3937_0",
  //       source: "1",
  //       target: "3937",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Trinity",
  //       },
  //       // id: "1_0_0_3938_0",
  //       source: "1",
  //       target: "3938",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Trinity",
  //       },
  //       // id: "1_0_0_3939_0",
  //       source: "1",
  //       target: "3939",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Agent Smith",
  //       },
  //       // id: "2_0_0_3937_0",
  //       source: "2",
  //       target: "3937",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Agent Smith",
  //       },
  //       // id: "2_0_0_3938_0",
  //       source: "2",
  //       target: "3938",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Agent Smith",
  //       },
  //       // id: "2_0_0_3939_0",
  //       source: "2",
  //       target: "3939",
  //       direction: "IN",
  //     },
  //     {
  //       label: "acted_in",
  //       properties: {
  //         role: "Old Salty Dog / Mr. Meeks / Prescient 1",
  //       },
  //       // id: "2_0_0_3941_0",
  //       source: "2",
  //       target: "3941",
  //       direction: "IN",
  //     },
  //   ],
  //   nodes: [
  //     {
  //       label: "person",
  //       properties: {
  //         born: 1961,
  //         id: 2,
  //         name: "Laurence Fishburne",
  //         poster_image:
  //           "https://image.tmdb.org/t/p/w185/mh0lZ1XsT84FayMNiT6Erh91mVu.jpg",
  //       },
  //       id: "0",
  //     },
  //     {
  //       label: "movie",
  //       properties: {
  //         duration: 136,
  //         id: 1,
  //         poster_image:
  //           "http://image.tmdb.org/t/p/w185/gynBNzwyaHKtXqlEKKLioNkjKgN.jpg",
  //         rated: "R",
  //         summary:
  //           "Thomas A. Anderson is a man living two lives. By day he is an average computer programmer and by night a malevolent hacker known as Neo who finds himself targeted by the police when he is contacted by Morpheus a legendary computer hacker who reveals the shocking truth about our reality.",
  //         tagline: "Welcome to the Real World.",
  //         title: "The Matrix",
  //       },
  //       id: "3937",
  //     },
  //     {
  //       label: "movie",
  //       properties: {
  //         duration: 138,
  //         id: 28,
  //         poster_image:
  //           "http://image.tmdb.org/t/p/w185/ezIurBz2fdUc68d98Fp9dRf5ihv.jpg",
  //         rated: "R",
  //         summary:
  //           "Six months after the events depicted in The Matrix Neo has proved to be a good omen for the free humans as more and more humans are being freed from the matrix and brought to Zion the one and only stronghold of the Resistance. Neo himself has discovered his superpowers including super speed ability to see the codes of the things inside the matrix and a certain degree of pre-cognition. But a nasty piece of news hits the human resistance: 250000 machine sentinels are digging to Zion and would reach them in 72 hours. As Zion prepares for the ultimate war Neo Morpheus and Trinity are advised by the Oracle to find the Keymaker who would help them reach the Source. Meanwhile Neo's recurrent dreams depicting Trinity's death have got him worried and as if it was not enough Agent Smith has somehow escaped deletion has become more powerful than before and has fixed Neo as his next target.",
  //         tagline: "Free your mind.",
  //         title: "The Matrix Reloaded",
  //       },
  //       id: "3938",
  //     },
  //     {
  //       label: "movie",
  //       properties: {
  //         duration: 129,
  //         id: 68,
  //         poster_image:
  //           "http://image.tmdb.org/t/p/w185/sKogjhfs5q3azmpW7DFKKAeLEG8.jpg",
  //         rated: "R",
  //         summary:
  //           "The human city of Zion defends itself against the massive invasion of the machines as Neo fights to end the war at another front while also opposing the rogue Agent Smith.",
  //         tagline: "Everything that has a beginning has an end.",
  //         title: "The Matrix Revolutions",
  //       },
  //       id: "3939",
  //     },
  //     {
  //       label: "person",
  //       properties: {
  //         born: 1967,
  //         id: 3,
  //         name: "Carrie-Anne Moss",
  //         poster_image:
  //           "https://image.tmdb.org/t/p/w185/8iATAc5z5XOKFFARLsvaawa8MTY.jpg",
  //       },
  //       id: "1",
  //     },
  //     {
  //       label: "person",
  //       properties: {
  //         born: 1960,
  //         id: 4,
  //         name: "Hugo Weaving",
  //         poster_image:
  //           "https://image.tmdb.org/t/p/w185/3DKJSeTucd7krnxXkwcir6PgT88.jpg",
  //       },
  //       id: "2",
  //     },
  //     {
  //       label: "movie",
  //       properties: {
  //         duration: 172,
  //         id: 130,
  //         poster_image:
  //           "http://image.tmdb.org/t/p/w185/k9gWDjfXM80iXQLuMvPlZgSFJgR.jpg",
  //         rated: "R",
  //         summary: "placeholder text",
  //         tagline: "Everything is Connected",
  //         title: "Cloud Atlas",
  //       },
  //       id: "3941",
  //     },
  //   ],
  // };

  const { jsonResult, viewResult } = excecuteResult || {};
  const { visible, onShow, onClose } = useVisible({ defaultVisible: false });
  const { nodes = [], edges = [] } = viewResult || {};
  const [state, setState] = useImmer<{
    tableType: "nodes" | "edges";
    currentLayout: Layout;
    graphCanvasContextValue: GraphinContextType;
    activeKey: string;
    properties: Record<string, string | boolean | any>;
    activeElementType: "node" | "edge" | any;
    tagName: string;
    id: string;
    formDisable: boolean;
    propertyTypes: Array<Record<string, string | boolean | any>>;
    editEdgeParams: EditEdge;
    selectType: "node" | "edge";
    selectProperties: any;
    sourceProperity: any;
    targetProperity: any;
    sourcePrimaryKey: string;
    targetPrimaryKey: string;
    currentData?: any;
  }>({
    tableType: "nodes",
    currentLayout: { type: "graphin-force", animation: false },
    graphCanvasContextValue: GraphCanvasContextInitValue,
    activeKey: "canvas",
    properties: {},
    activeElementType: "node",
    tagName: "",
    id: "",
    formDisable: true,
    propertyTypes: [],
    editEdgeParams: {},
    selectType: "node",
    selectProperties: [],
    sourceProperity: nodes,
    targetProperity: nodes,
    sourcePrimaryKey: "",
    targetPrimaryKey: "",
    currentData: {},
  });

  const {
    tableType,
    currentLayout,
    graphCanvasContextValue,
    activeKey,
    properties,
    activeElementType,
    tagName,
    id,
    formDisable,
    propertyTypes,
    editEdgeParams,
    selectType,
    selectProperties,
    sourceProperity,
    targetProperity,
    sourcePrimaryKey,
    targetPrimaryKey,
    currentData,
  } = state;
  const [form] = Form.useForm();
  const [addForm] = Form.useForm();
  const onTableTypeChange = (e: any) => {
    setState((draft) => {
      draft.tableType = e.target.value;
    });
  };
  const onLayoutChange = useCallback((layout: Layout) => {
    setState((draft) => {
      draft.currentLayout = { ...layout };
    });
  }, []);
  const getGraphCanvasContextValue = useCallback((contextValue: any) => {
    setState((draft) => {
      if (contextValue) {
        draft.graphCanvasContextValue = contextValue;
      }
    });
  }, []);

  useEffect(() => {
    if (excecuteResult) {
      const canvasContainer = document.querySelector(`.canvas`);
      const objResizeObserver = new ResizeObserver((entries) => {
        window.requestAnimationFrame(() => {
          if (!Array.isArray(entries) || !entries.length) {
            return;
          }

          entries?.forEach((entry) => {
            const cr = entry?.contentRect;
            if (
              graphCanvasContextValue?.graph &&
              graphCanvasContextValue?.graph?.destroyed === false
            ) {
              graphCanvasContextValue?.graph?.changeSize(cr.width, cr.height);
              graphCanvasContextValue?.graph?.fitCenter();
            }
          });
        });
      });
      if (canvasContainer) {
        objResizeObserver.observe(canvasContainer);
      }
      graphCanvasContextValue.graph?.on("node:click", (val) => {
        setState((draft) => {
          draft.id = val.item._cfg.id;
          draft.tagName = val.item._cfg.model.label;
          draft.activeElementType = val.item._cfg.type;
          draft.properties = { ...val.item._cfg.model.properties };
          draft.propertyTypes = find(
            graphData.nodes,
            (node) => node.labelName === val.item._cfg.model.label
          )?.properties;
          form.setFieldsValue({ ...val.item._cfg.model.properties });
          onShow();
        });
      });
      graphCanvasContextValue.graph?.on("edge:click", (val) => {
        setState((draft) => {
          draft.id = val.item._cfg.id;
          draft.tagName = val.item._cfg.model.label;
          draft.activeElementType = val.item._cfg.type;
          draft.properties = { ...val.item._cfg.model.properties };
          form.setFieldsValue({ ...val.item._cfg.model.properties });
          draft.propertyTypes = find(
            graphData.nodes,
            (node) => node.labelName === tagName
          )?.properties;
          const sourceTargetParams = editEdgeParamsTransform(
            val.item._cfg.source._cfg.model,
            val.item._cfg.target._cfg.model,
            graphData.nodes
          );
          draft.editEdgeParams = {
            graphName,
            labelName: val.item._cfg.model.label,
            ...sourceTargetParams,
          };
          onShow();
        });
      });
      graphCanvasContextValue.apis?.handleAutoZoom();
      return () => {
        if (canvasContainer) {
          objResizeObserver.unobserve(canvasContainer);
        }
      };
    }
  }, [graphCanvasContextValue, record]);
  useEffect(() => {
    if (viewResult) {
      setState((draft) => {
        //  draft.activeKey = "canvas";
        draft.sourceProperity = nodes;
        draft.targetProperity = nodes;
        draft.currentData = cloneDeep(viewResult);
      });
      graphCanvasContextValue.apis?.handleAutoZoom();
    }
  }, [excecuteResult]);

  useEffect(() => {
    if (!isEmpty(record)) {
      if (["FINISHED", "RUNNING"].includes(record.status)) {
        setState((draft) => {
          draft.activeKey = "canvas";
        });
      } else {
        setState((draft) => {
          draft.activeKey = "JSON";
        });
      }
    }
  }, [record]);

  const dealFormatData = (viewResult: { nodes: any; edges: any }) => {
    const newNodes = map(viewResult?.nodes, (item) => ({
      ...item,
      style: { label: { value: item.properties?.name || item.label } },
    }));
    const newEdge = map(viewResult?.edges, (item) => ({
      ...item,
      style: {
        label: {
          value: item.properties?.name || item.label,
          fill: "rgba(0,0,0,0.85)",
        },
      },
    }));
    return { nodes: newNodes, edges: newEdge };
  };
  const copyScript = (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-script`]}>
      <span>
        {$i18n.get({
          id: "openpiece-geaflow.job-detail.components.query.Operator",
          dm: "查询人",
        })}
        ：{record?.modifierName}
      </span>
      <span style={{ marginLeft: 16 }}>
        {$i18n.get({
          id: "openpiece-geaflow.job-detail.components.query.Time",
          dm: "查询时间",
        })}
        ：{record?.createTime}
      </span>
      <Popconfirm
        title={$i18n.get({
          id: "openpiece-geaflow.geaflow.graph-tabs.add.AreYouSureYouWant",
          dm: "你确定要删除吗?",
        })}
        onConfirm={() => {
          handleDelete();
        }}
      >
        <DeleteOutlined
          style={{ float: "right", lineHeight: "42px", height: 42 }}
        />
      </Popconfirm>
    </div>
  );
  const dealGraphData = (data) => {
    return {
      nodes: data.nodes,
      edges: Utils.processEdges(dealFormatData(data).edges, {
        poly: 50,
        loop: 10,
      }),
    };
  };

  return (
    <GraphCanvasContext.Provider value={{ ...graphCanvasContextValue }}>
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-excecute-result`]}>
        <Tabs
          type="card"
          tabPosition="left"
          activeKey={activeKey}
          style={{
            height: "100%",
          }}
          onChange={(val) => {
            setState((draft) => {
              draft.activeKey = val;
            });
          }}
        >
          {/* <TabPane
            key="JSONText"
            tab={
              <IconItem
                icon="icon-JSONwenben"
                name={$i18n.get({
                  id: "openpiece-geaflow.geaflow.query.JsonText",
                  dm: "JSON文本",
                })}
              />
            }
          >
            {copyScript}
            <pre style={{ whiteSpace: "break-spaces" }}>
              {JSONBig.stringify(jsonResult, null, 2)}
            </pre>
          </TabPane> */}

          <TabPane
            key="canvas"
            tab={
              <IconItem
                icon="icon-dianbiantupu"
                name={$i18n.get({
                  id: "openpiece-geaflow.geaflow.query.GraphView",
                  dm: "点边视图",
                })}
              />
            }
          >
            {copyScript}
            {record?.status === "RUNNING" ? (
              <div className={`canvas`} style={{ height: "100%" }}>
                <div className={`spin`}>
                  <Spin>
                    <Empty
                      image={Empty.PRESENTED_IMAGE_SIMPLE}
                      description={
                        <span>
                          {$i18n.get({
                            id: "openpiece-geaflow.job-detail.components.query.Running",
                            dm: "正在运行中请稍等",
                          })}
                        </span>
                      }
                    />
                  </Spin>
                </div>
              </div>
            ) : (
              <div className={`canvas`} style={{ height: "100%" }}>
                <GraphCanvas
                  key={queryId}
                  data={dealGraphData(dealFormatData(viewResult)) || {}}
                  layout={currentLayout}
                  getGraphCanvasContextValue={getGraphCanvasContextValue}
                  style={{ backgroundColor: "#fff" }}
                />
                <div className={styles[`${PUBLIC_PERFIX_CLASS}-tool-layout`]}>
                  <GraphCanvasTools />
                  <GraphCanvasLayout
                    currentLayout={currentLayout}
                    onLayoutChange={onLayoutChange}
                  />
                </div>
              </div>
            )}

            <SwitchDrawer
              visible={visible}
              onShow={onShow}
              onClose={onClose}
              position="right"
              width={300}
            >
              {id ? (
                <>
                  <div className={styles[`${PUBLIC_PERFIX_CLASS}-title`]}>
                    <span>{`ID：${id}`}</span>
                    <Tag>{tagName}</Tag>
                  </div>
                  <Form
                    className={styles[`${PUBLIC_PERFIX_CLASS}-form`]}
                    disabled={formDisable}
                    layout="vertical"
                    form={form}
                  >
                    {map(properties, (value, name) => {
                      const property = find(
                        propertyTypes,
                        (item) => item.name === name
                      );
                      if (PROPERTY_TYPE[property?.type] === "boolean") {
                        return (
                          <Form.Item
                            label={
                              <>
                                <div>{name}</div>
                                <Tooltip title="复制">
                                  <IconFont
                                    type="icon-fuzhi1"
                                    className={
                                      styles[`${PUBLIC_PERFIX_CLASS}-icon-copy`]
                                    }
                                    onClick={() => {
                                      copy(form.getFieldValue(name));
                                    }}
                                  />
                                </Tooltip>
                              </>
                            }
                            name={name}
                            rules={[
                              {
                                required: !property?.optional,
                                message: `${name}是必填项`,
                              },
                            ]}
                            required={false}
                          >
                            <Select disabled={formDisable}>
                              <Select.Option value={true}>是</Select.Option>
                              <Select.Option value={false}>否</Select.Option>
                            </Select>
                          </Form.Item>
                        );
                      }
                      if (PROPERTY_TYPE[property?.type] === "number") {
                        return (
                          <Form.Item
                            label={
                              <>
                                <div>{name}</div>
                                <Tooltip title="复制">
                                  <IconFont
                                    type="icon-fuzhi1"
                                    className={
                                      styles[`${PUBLIC_PERFIX_CLASS}-icon-copy`]
                                    }
                                    onClick={() => {
                                      copy(form.getFieldValue(name));
                                    }}
                                  />
                                </Tooltip>
                              </>
                            }
                            name={name}
                            rules={[
                              {
                                required: !property?.optional,
                                message: `${name}是必填项`,
                              },
                            ]}
                            required={false}
                          >
                            <InputNumber disabled={formDisable} />
                          </Form.Item>
                        );
                      }
                      return (
                        <Form.Item
                          label={
                            <>
                              <div>{name}</div>
                              <Tooltip title="复制">
                                <IconFont
                                  type="icon-fuzhi1"
                                  className={
                                    styles[`${PUBLIC_PERFIX_CLASS}-icon-copy`]
                                  }
                                  onClick={() => {
                                    copy(form.getFieldValue(name));
                                  }}
                                />
                              </Tooltip>
                            </>
                          }
                          name={name}
                          rules={[
                            {
                              required: !property?.optional,
                              message: `${name}是必填项`,
                            },
                          ]}
                          required={false}
                        >
                          <Input disabled={formDisable} />
                        </Form.Item>
                      );
                    })}
                  </Form>
                </>
              ) : (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
              )}
            </SwitchDrawer>
          </TabPane>
          <TabPane
            key="JSON"
            tab={
              <IconItem
                icon="icon-read"
                name={$i18n.get({
                  id: "openpiece-geaflow.geaflow.query.JsonView",
                  dm: "JSON视图",
                })}
              />
            }
          >
            {copyScript}
            <ReactJSONView src={jsonResult} displayDataTypes={false} />
          </TabPane>
        </Tabs>
      </div>
    </GraphCanvasContext.Provider>
  );
};

export default ExecuteResult;
