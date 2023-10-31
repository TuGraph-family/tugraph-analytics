import {Request, Response} from 'express';
import {parse} from 'url';

function deleteFlameGraph(req: Request, res: Response) {

  let result = {
    success: false,
    message: "123"
  }
  return res.json(result);
}

export default {
  'DELETE /proxy/:agentUrl/rest/flame-graphs': deleteFlameGraph,
};
