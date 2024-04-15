# TuGraph Analytics

[![Star](https://shields.io/github/stars/tugraph-family/tugraph-analytics?logo=startrek&label=Star&color=yellow)](https://github.com/TuGraph-family/tugraph-analytics/stargazers)
[![Fork](https://shields.io/github/forks/tugraph-family/tugraph-analytics?logo=forgejo&label=Fork&color=orange)](https://github.com/TuGraph-family/tugraph-analytics/forks)
[![Contributor](https://shields.io/github/contributors/tugraph-family/tugraph-analytics?logo=actigraph&label=Contributor&color=abcdef)](https://github.com/TuGraph-family/tugraph-analytics/contributors)
[![Commit](https://badgen.net/github/last-commit/tugraph-family/tugraph-analytics/master?icon=git&label=Commit)](https://github.com/TuGraph-family/tugraph-analytics/commits/master)
[![Docker](https://shields.io/docker/pulls/tugraph/geaflow-console?logo=docker&label=Docker&color=blue)](https://hub.docker.com/r/tugraph/geaflow-console/tags)
[![License](https://shields.io/github/license/tugraph-family/tugraph-analytics?logo=apache&label=License&color=blue)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Release](https://shields.io/github/v/release/tugraph-family/tugraph-analytics.svg?logo=stackblitz&label=Version&color=red)](https://github.com/TuGraph-family/tugraph-analytics/releases)
[![CN](https://shields.io/badge/Docs-中文-blue?logo=readme)](https://tugraph-analytics.readthedocs.io/en/latest/docs-cn/introduction/)
[![EN](https://shields.io/badge/Docs-English-blue?logo=readme)](https://tugraph-analytics.readthedocs.io/en/latest/docs-en/introduction/)
[![Blog](https://badgen.net/static/Blog/github.io/orange?icon=rss)](https://tugraph-analytics.github.io/)

> 🌐️ English | [中文](README_cn.md)

<!--intro-start-->
## Introduction
**TuGraph Analytics** (alias: GeaFlow) is the [**fastest**](https://ldbcouncil.org/benchmarks/snb-bi/) open-source OLAP graph database developed by Ant Group. It supports core capabilities such as trillion-level graph storage, hybrid graph and table processing, real-time graph computation, and interactive graph analysis. Currently, it is widely used in scenarios such as data warehousing acceleration, financial risk control, knowledge graph, and social networks.

For more information about GeaFlow: [GeaFlow Introduction](docs/docs-en/introduction.md)

For GeaFlow design paper: [GeaFlow: A Graph Extended and Accelerated Dataflow System](https://dl.acm.org/doi/abs/10.1145/3589771)

## Features

* Distributed streaming graph computation
* Hybrid graph and table processing (SQL+GQL)
* Unified stream/batch/graph computation
* Trillion-level graph-native storage
* Interactive graph analytics
* High availability and exactly once semantics
* High-level API operator development
* UDF/graph-algorithms/connector support
* One-stop graph development platform
* Cloud-native deployment

## Quick start

1. Prepare Git、JDK8、Maven、Docker environment。
2. Download Code：`git clone https://github.com/TuGraph-family/tugraph-analytics`
3. Build Project：`mvn clean install -DskipTests`
4. Test Job：`./bin/gql_submit.sh --gql geaflow/geaflow-examples/gql/loop_detection.sql`
3. Build Image：`./build.sh --all`
4. Start Container：`docker run -d --name geaflow-console -p 8888:8888 geaflow-console:0.1`

For more details：[Quick Start](docs/docs-cn/quick_start.md)。

## Development Manual

GeaFlow supports two sets of programming interfaces: DSL and API. You can develop streaming graph computing jobs using GeaFlow's SQL extension language SQL+ISO/GQL or use GeaFlow's high-level API programming interface to develop applications in Java.
* DSL application development: [DSL Application Development](docs/docs-en/application-development/dsl/overview.md)
* API application development: [API Application Development](docs/docs-en/application-development/api/overview.md)

## Real-time Capabilities

Compared with traditional stream processing engines such as Flink and Storm, which use tables as their data model for real-time processing, GeaFlow's graph-based data model has significant performance advantages when handling join relationship operations, especially complex multi-hops relationship operations like those involving 3 or more hops of join and complex loop searches.

[![total_time](./docs/static/img/vs_join_total_time_en.jpg)](./docs/docs-en/principle/vs_join.md)

[Why using graphs for relational operations is more appealing than table joins?](./docs/docs-en/principle/vs_join.md)

Association Analysis Demo Based on GQL:

```roomsql
--GQL Style
Match (s:student)-[sc:selectCource]->(c:cource)
Return c.name
;
```

Association Analysis Demo Based on SQL:

```roomsql
--SQL Style
SELECT c.name
FROM course c JOIN selectCourse sc 
ON c.id = sc.targetId
JOIN student s ON sc.srcId = s.id
;
```

## Contribution
Thank you very much for contributing to GeaFlow, whether bug reporting, documentation improvement, or major feature development, we warmly welcome all contributions. 

For more information: [Contribution](docs/docs-en/contribution.md).

## Partners
<table cellspacing="0" cellpadding="0">
  <tr align="center">
    <td height="80"><a href="https://github.com/CGCL-codes/YiTu"><img src="docs/static/img/partners/hust.png" width="300" alt="HUST" /></a></td>
    <td height="80"><a href="http://kw.fudan.edu.cn/"><img src="docs/static/img/partners/fu.png" width="300" alt="FU" /></a></td>
    <td height="80"><img src="docs/static/img/partners/zju.png" width="300" alt="ZJU" /></td>
  </tr>
  <tr align="center">
    <td height="80"><a href="http://www.whaleops.com/"><img src="docs/static/img/partners/whaleops.png" width="300" alt="WhaleOps" /></a></td>
    <td height="80"><a href="https://github.com/oceanbase/oceanbase"><img src="docs/static/img/partners/oceanbase.png" width="300" alt="OceanBase" /></a></td>
    <td height="80"><a href="https://github.com/secretflow/secretflow"><img src="docs/static/img/partners/secretflow.png" width="300" alt="SecretFlow" /></a></td>
  </tr>
</table>

## Contact Us
You can contact us through the following methods:

![contacts](docs/static/img/contacts-en.png)

**If you are interested in GeaFlow, please give our project a [ ⭐️ ](https://github.com/TuGraph-family/tugraph-analytics).**

## Acknowledgement
Thanks to some outstanding open-source projects in the industry, such as Apache Flink, Apache Spark, and Apache Calcite, some modules of GeaFlow were developed with their references. We would like to express our special gratitude for their contributions.
<!--intro-end-->