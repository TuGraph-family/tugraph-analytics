import { useCallback, useState } from "react";

export const useVisible = (options?: {
  afterShow?: () => void;
  afterClose?: () => void;
  defaultVisible?: boolean;
}) => {
  const { afterClose, afterShow, defaultVisible } = options || {};
  const [visible, setVisible] = useState(!!defaultVisible);
  const onShow = useCallback(() => {
    setVisible(true);
    if (afterShow) {
      afterShow();
    }
  }, []);
  const onClose = useCallback(() => {
    setVisible(false);
    if (afterClose) {
      afterClose();
    }
  }, []);
  return {
    visible,
    onShow,
    onClose,
  };
};
