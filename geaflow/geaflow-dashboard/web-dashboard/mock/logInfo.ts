import {Request, Response} from 'express';
import {parse} from 'url';

// mock tableInfoDataSource
const genLogInfo = (req: Request) => {
  let params = req.query;
  let current = Number(params.pageNo);
  let size = Number(params.pageSize);
  let total = 102400;

  let logInfo: string = "";
  for (let i = (current - 1) * size; i < Math.min(total, (current + 1) * size); i++) {
    logInfo += "log " + i + "\t";
    if (i % 10 == 0) {
      logInfo += "\n"
    }
  }
  console.log(params);
  return {
    total: total,
    data: logInfo
  }
}

function getLogInfo(req: Request, res: Response) {

  let result = {
    data: genLogInfo(req),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /proxy/:agentUrl/rest/logs/content': getLogInfo,
};
