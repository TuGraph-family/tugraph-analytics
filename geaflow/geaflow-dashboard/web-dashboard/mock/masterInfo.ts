import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genMasterInfo = () => {
  const metrics: API.ComponentInfo = {
    id: 0,
    name: "master",
    host: "localhost",
    pid: 123,
    agentPort: 8099
  };
  return metrics;
};

function getMasterInfo(req: Request, res: Response) {

  let result = {
    data: genMasterInfo(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /rest/master/info': getMasterInfo,
};
