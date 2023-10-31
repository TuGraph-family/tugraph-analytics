import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genLogList = () => {
  const logList: API.FileInfo[] = [
    {
      path: "/tmp/geaflow/logs/geaflow/client.log",
      size: 89128
    },
    {
      path: "/tmp/geaflow/logs/geaflow.log",
      size: 21491212
    }
  ];
  return logList;
};

function getLogList(req: Request, res: Response) {

  let result = {
    data: genLogList(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /proxy/:agentUrl/rest/logs': getLogList,
};
