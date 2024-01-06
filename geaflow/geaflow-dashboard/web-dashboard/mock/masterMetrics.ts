import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genMaterMetrics = () => {
  const metrics: API.ProcessMetrics = {
    heapCommittedMB: 1024,
    heapUsedMB: 1024,
    heapUsedRatio: 100,
    totalMemoryMB: 1024,
    fgcCount: 2,
    fgcTime: 3000,
    gcTime: 5000,
    gcCount: 5,
    avgLoad: 100,
    availCores: 12,
    processCpu: 20,
    usedCores: 10,
    activeThreads: 100,
  };
  return metrics;
};

function getProcessMetrics(req: Request, res: Response) {

  let result = {
    data: genMaterMetrics(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /rest/master/metrics': getProcessMetrics,
};
