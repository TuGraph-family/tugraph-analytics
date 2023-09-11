# GeaFlow (品牌名TuGraph-Analytics)

[![Star](https://shields.io/github/stars/tugraph-family/tugraph-analytics?logo=startrek&label=Star&color=yellow)](https://github.com/TuGraph-family/tugraph-analytics/stargazers)
[![Fork](https://shields.io/github/forks/tugraph-family/tugraph-analytics?logo=forgejo&label=Fork&color=orange)](https://github.com/TuGraph-family/tugraph-analytics/forks)
[![Contributor](https://shields.io/github/contributors/tugraph-family/tugraph-analytics?logo=actigraph&label=Contributor&color=abcdef)](https://github.com/TuGraph-family/tugraph-analytics/contributors)
[![Commit](https://badgen.net/github/last-commit/tugraph-family/tugraph-analytics/master?icon=git&label=Commit)](https://github.com/TuGraph-family/tugraph-analytics/commits/master)
[![Docker](https://shields.io/docker/pulls/tugraph/geaflow-console?logo=docker&label=Docker&color=blue)](https://hub.docker.com/r/tugraph/geaflow-console/tags)
[![License](https://shields.io/github/license/tugraph-family/tugraph-analytics?logo=apache&label=License&color=blue)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Release](https://shields.io/github/v/release/tugraph-family/tugraph-analytics.svg?logo=stackblitz&label=Version&color=red)](https://github.com/TuGraph-family/tugraph-analytics/releases)
[![CN](https://shields.io/badge/Docs-中文-blue?logo=readme)](https://tugraph-analytics.readthedocs.io/en/latest/docs-cn/introduction/)
[![EN](https://shields.io/badge/Docs-English-blue?logo=readme)](https://tugraph-analytics.readthedocs.io/en/latest/docs-en/introduction/)
[![Blog](https://shields.io/badge/Blog-github.io-orange?logo=rss)](https://tugraph-analytics.github.io/)

[English Document](README.md)

[ReadTheDocs](https://tugraph-analytics.readthedocs.io/en/latest/docs-cn/introduction/)

<!--intro-start-->
## 介绍
GeaFlow(品牌名TuGraph-Analytics)是蚂蚁集团开源的分布式流式图计算引擎，目前广泛应用于数仓加速、金融风控、知识图谱以及社交网络等场景。GeaFlow
的核心能力是以图为数据模型的流式计算引擎同时兼具流批一体的能力。相比传统的流式计算引擎比如Flink、Storm这些以表为模型的实时处理系统而言，GeaFlow以图为数据模型，在处理Join关系运算，尤其是复杂多度的关系运算如3度以上的Join、复杂环路查找上具备极大的性能优势。流式图计算相比离线图计算提供了一种高时效性低延迟的图计算模式。GeaFlow同时也支持图表一体的实时分析处理，能同时处理表数据和图数据。关于GeaFlow使用场景的更多介绍请参考：[GeaFlow介绍文档](docs/docs-cn/introduction.md)。

## 特性

* 以图为模型的分布式流式计算引擎
* 高可用和exactly once保障
* 图表一体化分析处理
* 提供SQL + ISO/GQL开发语言
* UDF/图算法/Connector插件支持
* 高阶API开发支持
* 一站式图研发平台支持
* 云原生部署支持

和传统流计算引擎如Flink的异同点如下：

| 特性 | GeaFlow | Flink |
| -------- | -------- | -------- |
|  数据模型    | 以图为基本数据模型的流式计算引擎，能同时处理表模型数据     | 以表模型为基础的流式计算引擎     |
| 状态管理 | 同时支持流式状态管理和图数据状态管理| 支持流状态管理|
| Exactly once |支持 | 支持|
| Join支持 | 支持复杂多度Join关联运算 | 不适合复杂Join |
| 图算法支持| 原生支持图算法 | Flink gelly模块支持(目前已移除)|
| 查询语言| SQL + ISO/GQL| SQL |

[为什么使用图进行关联运算比表Join更具吸引力？](./docs/docs-cn/principle/vs_join.md)
[![total_time](./docs/static/img/vs_join_total_time_cn.jpg)](./docs/docs-cn/principle/vs_join.md)

GeaFlow相关设计参考论文：[GeaFlow: A Graph Extended and Accelerated Dataflow System](https://dl.acm.org/doi/abs/10.1145/3589771)

## 快速上手

您需要首先在github上fork一份GeaFlow的代码，然后尝试编译源码。GeaFlow编译依赖mvn和jdk8环境, 编译完成以后，接下来可以尝试在本机运行一个实时图计算的作业，体验一下流图计算作业运行方式。本地运行GeaFlow
作业需要依赖Docker环境，关于快速上手更详细的内容请参考：[快速上手文档](docs/docs-cn/quick_start.md)。

## 开发GeaFlow应用

GeaFlow支持DSL和API两套编程接口，您既可以通过GeaFlow提供的类SQL扩展语言SQL+ISO/GQL进行流图计算作业的开发，也可以通过GeaFlow的高阶API编程接口通过Java语言进行应用开发。关于DSL
应用开发的详细内容请参考：[DSL开发文档](docs/docs-cn/application-development/dsl/overview.md)，高阶API应用开发部分参考：[API应用开发](docs/docs-cn/application-development/api/guid.md)。

```roomsql
--GQL Style
Match (s:student)-[sc:selectCource]->(c:cource)
Return c.name
;
```

```roomsql
--SQL Style
SELECT c.name
FROM course c JOIN selectCourse sc 
ON c.id = sc.targetId
JOIN student s ON sc.srcId = s.id
;
```

## 使用文档
参考 [ReadTheDocs](https://tugraph-analytics.readthedocs.io/en/latest/docs-cn/introduction/)

## 参与贡献
非常感谢您参与到GeaFlow的贡献中来，无论是bug反馈还是文档完善，或者是大的功能点贡献，我们都表示热烈的欢迎。关于如何参与贡献，请参考：[参与贡献文档](docs/docs-cn/contribution.md)。

# 联系我们
您可以通过钉钉或者微信用户群联系我们。

**If you feel GeaFlow useful or interesting, please ⭐️ [Star](https://github.com/TuGraph-family/tugraph-analytics)
it on github.**

![dingding](docs/static/img/dingding.png)

![wechat](docs/static/img/wechat.png)

**Email:**  tugraph@service.alipay.com

## 感谢
GeaFlow开发过程中部分模块参考了一些业界优秀的开源项目，包括Apache Flink、Apache Spark以及Apache Calcite等, 这里表示特别的感谢。
<!--intro-end-->
