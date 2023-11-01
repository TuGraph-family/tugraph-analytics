import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genOverview = () => {
  const overview: API.ClusterOverview = {
    totalContainers: 10,
    totalDrivers:2,
    activeContainers: 10,
    activeDrivers:2,
    totalWorkers: 20,
    availableWorkers: 12,
    pendingWorkers: 1,
    usedWorkers: 8,
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
  'GET /rest/overview': getOverview,
};
