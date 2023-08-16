# GeaFlow(The brand name is TuGraph-Analytics)

[中文文档](README_cn.md) 

[ReadTheDocs](https://tugraph-analytics.readthedocs.io/en/latest/docs-cn/introduction/)
<!--intro-start-->
## Introduction
GeaFlow (brand name TuGraph-Analytics) is an open-source distributed stream graph computing engine developed 
by Ant Group. It is currently widely used in scenarios such as data warehousing acceleration, financial risk
control, knowledge graphs, and social networks. GeaFlow's core capability is a stream computing engine based
on the graph as its data model, which also has the ability to handle both streaming and batch processing. 
Compared with traditional stream computing engines such as Flink and Storm, which use tables as their data
model for real-time processing, GeaFlow's graph-based data model has significant performance advantages when
handling join relationship operations, especially complex multi-degree relationship operations like those 
involving 3 or more degrees of join and complex loop searches. Stream graph computing provides a high-efficiency 
and low-latency graph computing mode compared to offline graph computing. GeaFlow also supports real-time analysis 
and processing of graphs and tables, and can handle both table and graph data. For more information on GeaFlow usage scenarios, 
please refer to: [GeaFlow introduction document](docs/docs-en/introduction.md)

## Features

* Distribute streaming graph computing.
* High availability and exactly once support.
* Graph and table integrated processing.
* Easy to develop with SQL + ISO/GQL.
* Pluggable for UDF、graph algorithm and connector.
* High level api support.
* One-stop graph development platform
* Cloud native deployment support.

The similarities and differences between GeaFlow and traditional stream computing engine, such as Flink, are as follows:

| Features | GeaFlow | Flink |
| -------- | -------- | -------- |
|  Data Model    | A graph-based stream computing engine that can handle both graph and table model data     | A stream computing engine based on the table model     |
| State Management | Supports both stream and graph data state management | Supports stream state management |
| Exactly once |Supported | Supported|
| Join Support | Supports complex multi-degree join operations | Not suitable for complex joins |
| Graph Algorithm Support| Native graph algorithm support | Flink Gelly module support (currently removed)|
| Query Language| SQL + ISO/GQL| SQL |

GeaFlow's relevant design reference papers are as follows: [GeaFlow: A Graph Extended and Accelerated Dataflow
System](https://dl.acm.org/doi/abs/10.1145/3589771)

## Quick start
You need to first fork a copy of GeaFlow code on Github and then try to compile the source code. Compiling GeaFlow 
requires mvn and JDK8 environment. You can then attempt to run a real-time graph computing job on your local machine 
to experience how the streaming graph computing job is run. Running a GeaFlow job locally requires a Docker 
environment. For more detailed information on how to get started quickly, please refer to the [quickstart document](docs/docs-en/quick_start.md).

## Develop GeaFlow Application
GeaFlow supports two sets of programming interfaces: DSL and API. You can develop streaming graph computing jobs 
using GeaFlow's SQL extension language SQL+ISO/GQL or use GeaFlow's high-level API programming interface to develop 
applications in Java. For more information on DSL application development, please refer to the [DSL development 
document](docs/docs-en/application-development/dsl/overview.md), and for the high-level API application development, please refer to the [API application development document](docs/docs-en/application-development/api/overview.md).

## Document

Please refer to:  [ReadTheDocs](https://tugraph-analytics.readthedocs.io/en/latest/docs-cn/introduction/)

## Contributing to GeaFlow
Thank you very much for contributing to GeaFlow, whether it's bug reporting, documentation improvement, or major 
feature development, we warmly welcome all contributions. For more information on how to contribute, please refer to 
our guidelines:[Contributing to GeaFlow](docs/docs-en/contribution.md).

## Contact Us
You can contact us through DingTalk or WeChat group.

**If you feel GeaFlow useful or interesting, please ⭐️ [Star](https://github.com/TuGraph-family/tugraph-analytics) 
it on github.**

![dingding](docs/static/img/dingding.png)

![wechat](docs/static/img/wechat.png)

**Email:**  tugraph@service.alipay.com

## Acknowledgement
Thanks to some outstanding open-source projects in the industry, such as Apache Flink, Apache Spark, and Apache Calcite, some modules of GeaFlow were developed with their references. We would like to express our special gratitude for their contributions.
<!--intro-end-->