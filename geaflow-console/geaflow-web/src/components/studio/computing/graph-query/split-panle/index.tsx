import React from "react";
import ReactSplitPane, { SplitPaneProps } from "react-split-pane";

interface ISplitPaneProps extends SplitPaneProps {
  children: React.ReactNode;
}

export const SplitPane: React.FC<ISplitPaneProps> = (props) => {
  return <ReactSplitPane {...props} />;
};
