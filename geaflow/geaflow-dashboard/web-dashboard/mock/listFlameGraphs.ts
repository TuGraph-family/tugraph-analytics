import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genFlameGraphList = () => {
  const logList: API.FileInfo[] = [
    {
      path: "/tmp/flame-graph/flame-graph-1.html",
      createdTime: new Date().getTime(),
      size: 89128
    },
    {
      path: "/tmp/flame-graph/flame-graph-2.html",
      createdTime: new Date().getTime() - 1000,
      size: 214912122
    }
  ];
  return logList;
};

function getFlameGraphList(req: Request, res: Response) {

  let result = {
    data: genFlameGraphList(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /proxy/:agentUrl/rest/flame-graphs': getFlameGraphList,
};
