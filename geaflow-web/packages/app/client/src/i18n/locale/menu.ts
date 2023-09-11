export const menuLocalConfig = {
  DEVELOPMENT: "图研发",
  Graphs: "图定义",
  Tables: "表定义",
  Edges: "边定义",
  Vertices: "点定义",
  Functions: "函数定义",
  Jobs: "图任务",
  OPERATION: "运维中心",
  Tasks: "作业管理",
  Instances: "实例管理",
  Files: "文件管理",
  Users: "用户管理",
  INSTALLATION: "一键安装",
  SYSTEM: "系统管理",
  Clusters: "集群管理",
  Tenants: "租户管理",
  Versions: "版本管理",
};

const convertDict = (dictionary: any) => {
  let result = {
    "en-US": {},
    "zh-CN": {},
  } as any;
  for (let key in dictionary) {
    let value = dictionary[key];
    result["en-US"][value] = key;
    result["zh-CN"][value] = value;
  }
  return result;
};

export const menuZhAndEnText = convertDict(menuLocalConfig);
