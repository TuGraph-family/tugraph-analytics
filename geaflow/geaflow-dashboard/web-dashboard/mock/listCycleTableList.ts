import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genComponentList = (current: number, pageSize: number) => {
  const tableListDataSource: API.CycleMetrics[] = [];

  for (let i = 0; i < pageSize; i += 1) {
    const index = (current - 1) * 10 + i;
    tableListDataSource.push({
      name: "cycle-" + index,
      opName: "opName-" + index,
      startTime: new Date().getTime()- (1000 * index),
      duration: index,
      totalTasks: 10,
      slowestTask: 1,
      slowestTaskExecuteTime: 100,
      inputRecords: 1000,
      outputRecords: 1000,
      inputKb: 999,
      outputKb: 999,
      avgGcTime: 2,
      avgExecuteTime: 99
    });
  }
  tableListDataSource.reverse();
  return tableListDataSource;
};

let tableListDataSource = genComponentList(1, 100);

function getCycleMetrics(req: Request, res: Response, u: string) {
  const result = {
    data: tableListDataSource,
    success: true
  };

  return res.json(result);
}

export default {
  'GET /rest/pipelines/:id/cycles': getCycleMetrics,
};
