import { Avatar } from "antd";
import MessageView from "../MessageView";
import styles from "./index.less";
import { UserOutlined } from "@ant-design/icons";

const avatarStyle = { backgroundColor: "#005EFF", verticalAlign: "middle" };

const SenderItem = ({ userName, item }) => {
  return (
    <div className={styles["sender-item"]}>
      {/* sender */}
      <MessageView isSender={true} {...item} />
      <div className={styles["avatar-wrap"]}>
        <Avatar size="large" style={avatarStyle}>
          <UserOutlined style={{ fontSize: 20 }} />
        </Avatar>
      </div>
    </div>
  );
};

export default SenderItem;
