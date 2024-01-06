## 介绍

Geaflow-dashboard为Geaflow提供作业级别的监控页面，可以轻松地通过dashboard查看作业的以下信息：
* 作业的健康度（Container和Worker活跃度）
* 作业的进度（Pipeline和Cycle信息）
* 作业各个组件的实时日志
* 作业各个组件的进程指标
* 作业各个组件的火焰图
* 作业各个组件的Thread Dump

### 后端服务
后端服务通过[Jetty](https://eclipse.dev/jetty)实现，由master组件对外开放RESTful的API接口，提供进程级别的信息查询。默认端口8090。

另外，在作业的每个pod中，还会启动一个agent，用于进阶的操作，例如执行火焰图分析和thread-dump，默认端口8089。
该端口不对外开放，由master的API服务进行代理访问。

### 前端服务
前端服务通过[React](https://react.dev)框架实现。build后的产物可以直接通过后端的Jetty进行代理访问，也可以通过web-dashboard/server.js手动启动。
当手动启动时，需要安装[node.js](https://nodejs.org)。

## 快速入门

1. 同时构建前端和后端
```shell
mvn clean install -DskipTests
```
通过后端Jetty服务代理请访问`http://localhost:8090`
2. 仅构建前端
```shell
cd web-dashboard
yarn install
yarn run build
```
通过node.js代理请访问`http://localhost:8081`。

前端构建产物目录为web-dashboard/resources/dist。