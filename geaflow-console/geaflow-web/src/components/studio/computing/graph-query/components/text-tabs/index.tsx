import { Tooltip } from "antd";
import { join, map } from "lodash";
import React, { useCallback, useEffect } from "react";
import { useImmer } from "use-immer";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";

import styles from "./index.module.less";

export interface TextTabsTab<T> {
  text: React.ReactNode;
  key: T;
  disabled?: boolean;
  description?: string;
}
interface TextTabsProps {
  tabs: TextTabsTab<any>[];
  onChange?: (key: string) => void;
  defaultActiveTab?: string;
  type?: "card" | "text";
  activeTab?: string;
  autoWidth?: boolean;
}

export const TextTabs: React.FC<TextTabsProps> = ({
  tabs,
  onChange,
  defaultActiveTab,
  type = "text",
  activeTab,
  autoWidth = true,
}) => {
  const [state, setState] = useImmer<{ activeTab: string }>({
    activeTab: defaultActiveTab || tabs[0].key,
  });

  useEffect(() => {
    if (activeTab) {
      setState((draft) => {
        draft.activeTab = activeTab;
      });
    }
  }, [activeTab]);

  const onTabClick = useCallback(
    (tab: TextTabsTab<any>) => {
      setState((draft) => {
        draft.activeTab = tab.key;
      });
      if (onChange) {
        onChange(tab.key);
      }
    },
    [onChange, setState]
  );
  return (
    <div
      className={join(
        [
          styles[`${PUBLIC_PERFIX_CLASS}-text-tabs`],
          styles[`${PUBLIC_PERFIX_CLASS}-text-tabs-${type}`],
          "text-tabs",
          `text-tabs-${type}`,
          styles[
            `${PUBLIC_PERFIX_CLASS}-text-tabs-${autoWidth ? "" : "inline"}`
          ],
        ],
        " "
      )}
    >
      {map(tabs, (tab) => {
        const { disabled, key, text } = tab;
        const isActiveTab = state.activeTab === key;
        return (
          <div
            className={
              isActiveTab
                ? join(
                    [
                      styles[`${PUBLIC_PERFIX_CLASS}-text-tabs-item`],
                      styles[`${PUBLIC_PERFIX_CLASS}-text-tabs-item-active`],
                      "text-tabs-item",
                      "text-tabs-item-active",
                    ],
                    " "
                  )
                : join(
                    [
                      styles[`${PUBLIC_PERFIX_CLASS}-text-tabs-item`],
                      "text-tabs-item",
                    ],
                    " "
                  )
            }
            key={key}
            onClick={disabled ? undefined : () => onTabClick(tab)}
            style={{ cursor: disabled ? "not-allowed" : "pointer" }}
          >
            {isActiveTab && (
              <span className={styles[`${PUBLIC_PERFIX_CLASS}-outer-left`]} />
            )}
            <Tooltip title={tab?.description}>{text}</Tooltip>
            {isActiveTab && (
              <span className={styles[`${PUBLIC_PERFIX_CLASS}-outer-right`]} />
            )}
          </div>
        );
      })}
    </div>
  );
};

export default TextTabs;
