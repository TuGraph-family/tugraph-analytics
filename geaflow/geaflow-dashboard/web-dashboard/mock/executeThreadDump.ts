import {Request, Response} from 'express';
import {parse} from 'url';

function executeThreadDump(req: Request, res: Response) {

  let result = {
    success: true
  }
  return res.json(result);
}

export default {
  'POST /proxy/:agentUrl/rest/thread-dump': executeThreadDump,
};
