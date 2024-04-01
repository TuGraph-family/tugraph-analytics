# 名称解释

**图**：图用于展示不同变量之间的关系，通常包括节点（点）和边（线）两部分。节点代表一个个体或对象，边则代表它们之间的关系。图可以用来解释复杂的关系网络和信息流动，如社交网络、交通网络、物流网络等。常见的图形类型包括有向图、无向图、树形图、地图等。

**K8S**：k8s是[Kubernetes](https://kubernetes.io/)的简称，是一个开源的容器编排平台，提供了自动化部署、自动扩展、自动管理容器化应用程序的功能。它可以在各种云平台、物理服务器和虚拟机上运行，支持多种容器运行时，可以实现高可用性、负载均衡、自动扩容、自动修复等功能。

**Graph Processing**： Graph Processing是一种计算模型，用于处理图形数据结构的计算问题。图计算模型可以用于解决许多现实世界的问题，例如社交网络分析、网络流量分析、医疗诊断等，典型的系统有 [Apache Giraph](https://giraph.apache.org/), [Spark GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html)。

**DSL**： DSL是领域特定语言（Domain-Specific Language）的缩写。它是一种针对特定领域或问题的编程语言，与通用编程语言不同，DSL主要关注于解决特定领域的问题，并针对该领域的特定需求进行优化。DSL可以使得编程更加简单、高效，同时也能够提高代码的可读性和可维护性。下面的**Gremlin**、**ISO-GQL**就是DSL中的一种。

**HLA**: HLA 是 High level language 的缩写，与**DSL**不同，它使用通用语言进行编程，Geaflow目前只支持Java程序编写。它主要通过计算引擎SDK进行程序编写，执行方式是将程序整体打包交给引擎执行，对比**DSL**，它的执行方式更加灵活，但相对应的编程也会更加复杂。

**Gremlin**: [Gremlin](https://tinkerpop.apache.org/gremlin.html)是一种图形遍历语言，用于在图形数据库中进行数据查询和操作。它是一种声明式的、基于图的编程语言，可以用于访问各种类型的图形数据库，如Apache TinkerPop、Neo4j等。它提供了一组灵活的API，可以帮助开发者在图形数据库中执行各种操作，如遍历、过滤、排序、连接、修改等。

**ISO-GQL**：[GQL](https://www.gqlstandards.org/)是面向属性图的标准查询语言，全称是“图形查询语言”，其为ISO/IEC国际标准数据库语言。GeaFlow不仅支持了Gremlin查询语言，而且还支持了GQL。

**Window**: 参考VLDB 2015 [Google Dataflow Model](https://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/43864.pdf)，窗口的概念在 Geaflow 中是其数据处理逻辑中的关键要素，用于统一有界和无界的数据处理。数据流统一被看成一个个窗口数据的集合，系统处理批次的粒度也就是窗口的粒度。

**Cycle**： GeaFlow Scheduler调度模型中的核心数据结构，一个cycle描述为可循环执行的基本单元，包含输入，中间计算和数据交换，输出的描述。由执行计划中的vertex group生成，支持嵌套。

**Event**： Runtime层调度和计算交互的核心数据结构，Scheduler将一系列Event集合构建成一个State Machine，将其分发到Worker上进行计算执行。其中有些Event是可执行的，即自身具备计算语义，整个调度和计算过程为异步执行。

**Graph Traversal** : Graph Traversal 是指遍历图数据结构中所有节点或者部分节点的过程，在特定的顺序下访问所有节点，主要是深度优先搜索（DFS）和 广度优先搜索（BFS）。用于解决许多问题，包括查找两个节点之间的最短路径、检测图中的循环等。

**Graph State**： GraphState 是用来存放Geaflow的图数据或者图迭代计算过程的中间结果，提供Exactly-Once语义，并提供作业级复用的能力。GraphState 分为 Static 和 Dynamic 两种，Static 的 GraphState 将整个图看做是一个完整的，所有操作都在一张全图上进行；Dynamic 的 GraphState 认为图动态变化的，由一个个时间切片构成，所有切片构成一个完整的图，所有的操作都是在切片上进行。

**Key State**： KeyState 用于存放计算过程中的中间结果，通常用于流式处理，例如执行aggregation时在KeyState中记录中间聚合结果。类似GraphState，Geaflow 会将 KeyState 定期持久化，因此KeyState也提供Exactly-Once语义。KeyState根据数据结果不同可以分为KeyValueState、KeyListState、KeyMapState等。