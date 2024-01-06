import { Form } from "antd";
import React from "react";
import { InstanceTable } from "./InstanceTable";
// @ts-ignore
import styles from "./list.module.less";

export const InstanceList: React.FC<{}> = ({}) => {
  const [form] = Form.useForm();

  return (
    <div className={styles["crowd-example"]}>
      <Form
        form={form}
        initialValues={{ name: "", version: "", ceator: "" }}
      >
        <InstanceTable form={form} />
      </Form>
    </div>
  );
};
