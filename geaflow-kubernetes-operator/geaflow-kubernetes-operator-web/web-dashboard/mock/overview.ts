import {Request, Response} from 'express';

// mock tableListDataSource
const genOverview = () => {
  const overview: API.ClusterOverview = {
    host: "localhost",
    namespace: "default",
    masterUrl: "http://127.0.0.1:8000",
    totalJobNum: 142,
    jobStateNumMap: {
      "RUNNING": 42,
      "FAILED": 40,
      "SUBMITTED": 30,
      "REDEPLOYING": 10,
      "FINISHED": 10,
      "SUSPENDED": 10
    }
  };
  return overview;
};

function getOverview(req: Request, res: Response) {

  let result = {
    data: genOverview(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /api/overview': getOverview,
};
