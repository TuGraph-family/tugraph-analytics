import { Tag, Tooltip } from "antd";
import { find, join, isArray } from "lodash";
import React, { useCallback, useEffect } from "react";
import { useImmer } from "use-immer";
import IconFont from "../icon-font";
import TextTabs, { TextTabsTab } from "../text-tabs";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import ExecuteResult from "../excecute-result";

import styles from "./index.module.less";

export interface ExcecuteQueryResult {
  result: {
    nodes?: Array<any>;
    edges?: Array<any>;
    message?: string;
  } | null;
  data: any;
  isSuccess: boolean;
  requestTime?: number;
}

interface ExcecuteHistoryProps {
  queryResultList: any;
  efficiencyResult?: any[];
  planResult?: any[];
  onResultClose?: (resultIndex?: string) => void;
  graphName: string;
  graphData: any;
  record?: any;
  handleDelete: () => void;
}

const ExcecuteResultPanle: React.FC<ExcecuteHistoryProps> = ({
  queryResultList,
  onResultClose,
  graphData,
  graphName,
  record,
  handleDelete,
}) => {
  const [state, setState] = useImmer<{
    tabs: any;
    activeTab?: string;
    isFullView: boolean;
    activeResult?: any;
    modalOpen: boolean;
    queryId: string;
  }>({
    tabs: [{ text: "执行结果", key: "result" }],
    activeTab: "",
    isFullView: false,
    modalOpen: false,
    queryId: "",
  });
  const { tabs, activeTab, isFullView, activeResult, modalOpen, queryId } =
    state;
  const onFullView = useCallback(() => {
    setState((draft) => {
      draft.isFullView = !isFullView;
    });
  }, [isFullView]);
  const fullViewButton = (
    <Tooltip
      title={isFullView ? "退出全屏" : "全屏显示"}
      placement={!isFullView ? "top" : "bottom"}
    >
      <IconFont
        type={isFullView ? "icon-shouqiquanping" : "icon-quanping"}
        onClick={onFullView}
      />
    </Tooltip>
  );

  useEffect(() => {
    if (queryResultList) {
      let latestResult = {};
      if (queryResultList[0]?.status === "FINISHED") {
        latestResult = queryResultList[0]?.result;
      } else {
        latestResult = {
          jsonResult: {
            result: queryResultList[0]?.result,
          },
        };
      }

      const newTabs = [
        ...(queryResultList || []).map((result, index) => ({
          key: result.id,
        })),
      ];
      const activeTab = newTabs[newTabs.length - 1].key;
      setState((draft) => {
        draft.tabs = newTabs as Array<{ text: React.ReactNode; key: string }>;
        draft.activeTab = activeTab;
        draft.activeResult = latestResult;
        draft.queryId = queryResultList[0]?.id;
      });
    }
  }, [queryResultList]);

  return (
    <div
      className={join(
        [
          styles[`${PUBLIC_PERFIX_CLASS}-excecute-history`],
          isFullView
            ? styles[`${PUBLIC_PERFIX_CLASS}-excecute-history__full`]
            : "",
        ],
        " "
      )}
    >
      <TextTabs
        type="card"
        tabs={tabs}
        activeTab={activeTab}
        autoWidth={false}
        // onChange={(val) => {
        //   setState((draft) => {
        //     draft.activeResult = find(
        //       queryResultList,
        //       (result) => result.id === val
        //     );
        //   });
        // }}
      />
      <div
        className={styles[`${PUBLIC_PERFIX_CLASS}-excecute-history-actions`]}
      >
        {fullViewButton}
      </div>
      <div
        className={styles[`${PUBLIC_PERFIX_CLASS}-excecute-history-content`]}
      >
        <ExecuteResult
          excecuteResult={activeResult}
          queryId={queryId}
          graphName={graphName}
          graphData={graphData}
          modalOpen={modalOpen}
          record={record}
          onClose={() => {
            setState((draft) => {
              draft.modalOpen = false;
            });
          }}
          handleDelete={handleDelete}
        />
      </div>
    </div>
  );
};

export default ExcecuteResultPanle;
