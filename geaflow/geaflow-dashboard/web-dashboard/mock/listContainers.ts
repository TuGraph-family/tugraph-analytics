import {Request, Response} from 'express';
import {parse} from 'url';
import {genComponentList} from "./listDrivers";

let tableListDataSource = genComponentList(1, 100, "container");

function getContainers(req: Request, res: Response, u: string) {
  const result = {
    data: tableListDataSource,
    success: true
  };

  return res.json(result);
}

export default {
  'GET /rest/containers': getContainers,
};
