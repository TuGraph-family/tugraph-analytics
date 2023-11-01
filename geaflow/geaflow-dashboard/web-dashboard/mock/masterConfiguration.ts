import {Request, Response} from 'express';

// mock tableListDataSource
const genConfig = () => {
  let configs: Record<string, any> = {};
  for (let i = 0; i < 100; i ++) {
    let name: string = "config-" + i;
    configs[name] = "value-" + i;
  }
  return configs;
};

function getMasterConfiguration(req: Request, res: Response) {

  let result = {
    data: genConfig(),
    success: true
  }
  return res.json(result);
}

export default {
  'GET /rest/master/configuration': getMasterConfiguration,
};
