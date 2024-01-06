/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

/**
 * Run "node server.js" command to start the webpage
 * after "npm run build" command.
 * @type {e | (() => Express)}
 */
const express = require('express')
const path = require('node:path')
const proxy = require("http-proxy-middleware").createProxyMiddleware
const app = express()
const port = 8000
app.use(express.static('resources/dist'))
app.use('/api', proxy({ target: 'http://localhost:8090/', changeOrigin: true }));
app.get('/*', function (req, res) {
  res.sendFile(path.join(__dirname, 'resources', 'static', 'index.html'));
});
app.listen(port, () => console.log(`Geaflow-kubernetes-operator web server starts with port ${port}!`))
