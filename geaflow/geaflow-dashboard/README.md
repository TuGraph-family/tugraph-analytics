## Introduction

Geaflow-dashboard provides a job-level monitoring page for Geaflow. You can easily view the following information of the job through the dashboard:
* Job health (Container and Worker activity)
* Job progress (Pipeline and Cycle information)
* Runtime logs of each component of the job
* Process metrics for each component of the job
* Flame graph of each component of the job
* Thread Dump of each component of the job

### Backend Server
The backend service is implemented by [Jetty](https://eclipse.dev/jetty), and the master component exposes 
the RESTful API to provide process-level information. The default port is 8090.

In addition, in each pod of the job, an agent will be started for advanced operations, such as 
performing flame-graph analysis and thread-dump. The default port is 8089.
This port can only be accessed by the master's API service.

### Frontend Page
Frontend services are implemented by the [React](https://react.dev) framework. The built product can be accessed 
directly through the back-end Jetty proxy, or it can be started manually through web-dashboard/server.js.
When started manually, [node.js](https://nodejs.org) needs to be installed.

## Quick Start

1. Build frontend and backend together.
```shell
mvn clean install -DskipTests
```
Please visit `http://localhost:8090` Via the backend Jetty service proxy.
2. Build the frontend only
```shell
cd web-dashboard
yarn install
yarn run build
node server.js
```
Please visit `http://localhost:8081` via node.js proxy.

The front-end build product directory is web-dashboard/resources/dist.