import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
export function genComponentList(current: number, pageSize: number, componentType: string) {
  const tableListDataSource: API.ComponentInfo[] = [];

  for (let i = 0; i < pageSize; i += 1) {
    const index = (current - 1) * 10 + i;
    tableListDataSource.push({
      id: index,
      name: componentType + "-" + index,
      host: "localhost",
      agentPort: 8089,
      pid: index,
      lastTimestamp: new Date().getTime() - (1000 * index),
      isActive: index % 5 != 0,
      metrics: {
        heapCommittedMB: 100,
        heapUsedMB: index,
        heapUsedRatio: index,
        totalMemoryMB: 1,
        fgcCount: 1,
        fgcTime: 1,
        gcTime: 1,
        gcCount: 1,
        avgLoad: 1,
        availCores: 100,
        processCpu: 100 - index,
        usedCores: 100 - index,
        activeThreads: 1
      }
    });
  }
  tableListDataSource.reverse();
  return tableListDataSource;
}

let tableListDataSource = genComponentList(1, 100, "driver");

function getDrivers(req: Request, res: Response, u: string) {
  const result = {
    data: tableListDataSource,
    success: true
  };

  return res.json(result);
}

export default {
  'GET /rest/drivers': getDrivers,
};
