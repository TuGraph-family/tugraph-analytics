import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableListDataSource
const genList = (current: number, pageSize: number) => {
  const tableListDataSource: API.PipelineMetrics[] = [];

  for (let i = 0; i < pageSize; i += 1) {
    const index = (current - 1) * 10 + i;
    tableListDataSource.push({
      name: "pipeline#" + index,
      startTime: new Date().getTime() - (index * 1000),
      duration: index
    });
  }
  tableListDataSource.reverse();
  return tableListDataSource;
};

let tableListDataSource = genList(1, 100);

function getPipelineMetrics(req: Request, res: Response, u: string) {

  const result = {
    data: tableListDataSource,
    success: true
  };

  return res.json(result);
}

export default {
  'GET /rest/pipelines': getPipelineMetrics,
};
