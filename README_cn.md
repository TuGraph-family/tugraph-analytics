# GeaFlow (品牌名TuGraph-Analytics)

[English Document](README.md)

<!--intro-start-->
## 介绍
GeaFlow(品牌名TuGraph-Analytics)是蚂蚁集团开源的分布式实时图计算引擎，目前广泛应用于金融风控、社交网络、知识图谱以及数据应用等场景。GeaFlow的核心能力是流式图计算,
流式图计算相比离线图计算提供了一种高时效性低延迟的图计算模式。相比传统的流式计算引擎比如Flink、Storm这些面向表数据的实时处理系统而言，GeaFlow
主要面向图数据的实时处理，支持更加复杂的关系分析计算，比如多度关系实时查找、环路检查等；同时也支持图表一体的实时分析处理，能同时处理表数据和图数据。关于GeaFlow使用场景更多介绍请参考：[GeaFlow介绍文档](docs/docs-cn/introduction.md)。

## 快速上手

您需要首先在github上fork一份GeaFlow的代码，然后尝试编译源码。GeaFlow编译依赖mvn和jdk8环境, 编译完成以后，接下来可以尝试在本机运行一个实时图计算的作业，体验一下流图计算作业运行方式。本地运行GeaFlow
作业需要依赖Docker环境，关于快速上手更详细的内容请参考：[快速上手文档](docs/docs-cn/quick_start.md)。

## 开发GeaFlow应用

GeaFlow支持DSL和API两套编程接口，您既可以通过GeaFlow提供的类SQL扩展语言SQL+ISO/GQL进行流图计算作业的开发，也可以通过GeaFlow的高阶API编程接口通过Java语言进行应用开发。关于DSL
应用开发的详细内容请参考：[DSL开发文档](docs/docs-cn/application-development/dsl/overview.md)，高阶API应用开发部分参考：[API应用开发](docs/docs-cn/application-development/api/guid.md)。

## 使用文档
以下为GeaFlow使用文档列表

- [GeaFlow介绍](docs/docs-cn/introduction.md)
- [快速上手](docs/docs-cn/quick_start.md)
- 基本概念:
    - [Graph View](docs/docs-cn/concepts/graph_view.md)
    - [Streaming Graph](docs/docs-cn/concepts/stream_graph.md)
    - [名词解释](docs/docs-cn/concepts/glossary.md)
- GeaFlow应用开发:
    - [API应用开发](docs/docs-cn/application-development/api/guid.md)
    - [DSL应用开发](docs/docs-cn/application-development/dsl/overview.md)
- [部署运行](docs/docs-cn/deploy/install_guid.md)
- GeaFlow原理介绍:
    - [Framework原理介绍](docs/docs-cn/principle/framework_principle.md)
    - [State原理介绍](docs/docs-cn/principle/state_principle.md)
    - [DSL原理介绍](docs/docs-cn/principle/dsl_principle.md)

## 参与贡献
非常感谢您参与到GeaFlow的贡献中来，无论是bug反馈还是文档完善，或者是大的功能点贡献，我们都表示热烈的欢迎。关于如何参与贡献，请参考：[参与贡献文档](docs/docs-cn/contribution.md)。

# 联系我们
您可以通过钉钉或者微信用户群联系我们。

![dingding](docs/static/img/dingding.png)

![wechat](docs/static/img/wechat.png)

## 感谢
GeaFlow开发过程中部分模块参考了一些业界优秀的开源项目，包括Apache Flink、Apache Spark以及Apache Calcite等, 这里表示特别的感谢。
<!--intro-end-->
