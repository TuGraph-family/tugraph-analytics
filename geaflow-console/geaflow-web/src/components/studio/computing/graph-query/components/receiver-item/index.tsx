import { Avatar } from "antd";
import MessageView from "../MessageView";
import { RobotOutlined } from "@ant-design/icons";
import styles from "./index.less";

const avatarStyle = { backgroundColor: "#005EFF", verticalAlign: "middle" };

const ReceiverItem = ({ userName, item, onCopy, onSend }) => {
  return (
    <div className={styles["receiver-item"]}>
      {/* receiver */}
      <div className={styles["avatar-wrap"]}>
        <Avatar size="large" style={avatarStyle}>
          <RobotOutlined style={{ fontSize: 20 }} />
        </Avatar>
      </div>
      <MessageView isSender={false} {...item} onCopy={onCopy} onSend={onSend} />
    </div>
  );
};

export default ReceiverItem;
