import { Application } from '@tugraph/openpiece-server';
import config from './config';

import path from 'path';
import fs from 'fs';
import Router from '@koa/router';
import koaStatic from 'koa-static';

const router = Router()
const app = new Application(config);

if (require.main === module) {
  app.runAsCLI();
}

router.get(/^(?!\/api)\/.*/, async (ctx, next) => {
  ctx.type = 'html';
  ctx.body = fs.createReadStream(path.resolve(__dirname, '../../client/dist/index.html'));
});

const filePath = path.resolve(__dirname, '../../client/dist');
console.log('path', filePath)

app.use(koaStatic(filePath));
app.use(router.routes()).use(router.allowedMethods());

export default app;
