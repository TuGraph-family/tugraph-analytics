import { join } from "lodash";
import React, { useMemo } from "react";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";
import IconFont from "../icon-font";

import styles from "./index.module.less";

export interface SwitchDrawerProps {
  children?: React.ReactNode;
  title?: React.ReactNode;
  width?: number;
  position?: "left" | "right";
  footer?: React.ReactNode;
  visible?: boolean;
  onClose?: () => void;
  onShow?: () => void;
  className?: string;
  style?: React.CSSProperties;
  backgroundColor?: string;
}

const SwitchDrawer: React.FC<SwitchDrawerProps> = (props) => {
  const {
    children,
    width,
    title,
    footer,
    visible,
    onShow,
    onClose,
    className,
    style,
    position = "right",
    backgroundColor,
  } = props;
  const drawerWidth = useMemo(() => width || 520, [width]);
  const wrapperStyles = useMemo(() => {
    let style: React.CSSProperties = {
      width: drawerWidth,
      backgroundColor: backgroundColor,
    };
    if (visible) {
      if (position === "left") {
        style.left = 0;
      } else {
        style.right = 0;
      }
    } else {
      if (position === "left") {
        style.left = -drawerWidth + 10;
      } else {
        style.right = -drawerWidth + 10;
      }
    }
    return style;
  }, [position, visible]);

  const getSwitcherArrow = () => {
    if (visible) {
      if (position === "left") {
        return <IconFont type="icon-jiantou" rotate={180} />;
      } else {
        return <IconFont type="icon-jiantou" />;
      }
    } else {
      if (position === "left") {
        return <IconFont type="icon-jiantou" />;
      } else {
        return <IconFont type="icon-jiantou" rotate={180} />;
      }
    }
  };
  return (
    <div
      className={join(
        [styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer`], className],
        " "
      )}
      style={{ ...style, ...wrapperStyles }}
    >
      <div className={styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-wrapper`]}>
        {title && (
          <div
            className={styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-header`]}
          >
            {title}
          </div>
        )}
        <div
          className={styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-body`]}
          style={{
            display: visible ? "block" : "none",
          }}
        >
          {children}
        </div>
        {footer && (
          <div
            className={styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-footer`]}
          >
            {footer}
          </div>
        )}
      </div>
      <div
        className={join(
          [
            styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-switcher`],
            styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-switcher-${position}`],
          ],
          " "
        )}
        onClick={visible ? onClose : onShow}
      >
        <div
          className={
            styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-switcher-arrow`]
          }
        >
          {getSwitcherArrow()}
        </div>
        <div
          className={styles[`${PUBLIC_PERFIX_CLASS}-switch-drawer-switcher-bg`]}
          style={{ backgroundColor: backgroundColor }}
        />
      </div>
    </div>
  );
};

export default SwitchDrawer;
