import {Request, Response} from 'express';
import {parse} from 'url';

function addFlameGraph(req: Request, res: Response) {

  let result = {
    success: true
  }
  return res.json(result);
}

export default {
  'POST /proxy/:agentUrl/rest/flame-graphs': addFlameGraph,
};
