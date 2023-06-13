# GeaFlow(The brand name is TuGraph-Analytics)

[中文文档](README_cn.md)
<!--intro-start-->
## Introduction
GeaFlow(the brand name is TuGraph-Analytics) is an open-source distributed real-time graph computing engine 
developed by Ant Group. It is widely used in scenarios such as financial risk control, social networks, knowledge 
graphs, and data applications. The core competence of GeaFlow is streaming graph computing, which provides a 
high-time efficiency and low-latency graph computing mode compared to offline graph computing. Compared with 
traditional streaming computing engines such as Flink and Storm, which are real-time processing systems for table 
data, GeaFlow mainly focuses on real-time processing of graph data, supporting more complex relationship analysis 
and calculations, such as real-time search for multi-degree relationships and loop detection. At the same time, it 
also supports real-time analysis and processing of graph-table integration and can handle both table data and graph 
data at the same time. For more information on GeaFlow use cases, please refer to the [GeaFlow introduction document](docs/docs-en/introduction.md)

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

Here is the document list for GeaFlow:

- [GeaFlow Introduction](docs/docs-en/introduction.md)
- [Quick start](docs/docs-en/quick_start.md)
- Concepts:
    - [Graph View](docs/docs-en/concepts/graph_view.md)
    - [Streaming Graph](docs/docs-en/concepts/stream_graph.md)
    - [Glossary](docs/docs-en/concepts/glossary.md)
- GeaFlow Development:
    - [API Development](docs/docs-en/application-development/api/guid.md)
    - [DSL Development](docs/docs-en/application-development/dsl/overview.md)
- [Deployment](docs/docs-en/deploy/install_guid.md)
- Principle introduction:
    - [Framework Principle](docs/docs-en/principle/framework_principle.md)
    - [State Principle](docs/docs-en/principle/state_principle.md)
    - [DSL Principle](docs/docs-en/principle/dsl_principle.md)

## Contributing to GeaFlow
Thank you very much for contributing to GeaFlow, whether it's bug reporting, documentation improvement, or major 
feature development, we warmly welcome all contributions. For more information on how to contribute, please refer to 
our guidelines:[Contributing to GeaFlow](docs/docs-en/contribution.md).

## Contact Us
You can contact us through DingTalk or WeChat group.

![dingding](docs/static/img/dingding.png)

![wechat](docs/static/img/wechat.png)
## Acknowledgement
Thanks to some outstanding open-source projects in the industry, such as Apache Flink, Apache Spark, and Apache Calcite, some modules of GeaFlow were developed with their references. We would like to express our special gratitude for their contributions.
<!--intro-end-->