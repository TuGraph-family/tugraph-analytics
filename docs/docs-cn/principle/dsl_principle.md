# DSL原理介绍

## GeaFlow DSL架构
GeaFlow DSL整体架构如下图所示：
![DSL架构](../../static/img/dsl_arch.png)
从上到下分五层：
* DSL语言层
  定义了GeaFlow DSL的语言，目前使用SQL + ISO/GQL融合分析语言。
* 统一Parser层
  DSL统一的语法解析层，主要将DSL文本转换成统一的AST语法树。GeaFlow在[Apache Calcite](https://calcite.apache.org/)的基础上扩展了GQL的语法，同时将SQL语法和GQL融合解析成统一的语法树。
* 统一逻辑执行计划
  GeaFlow在关系代数的基础之上扩展了一套图的逻辑执行计划，实现了图表逻辑执行计划的统一。
* 优化器
  在现有SQL RBO优化器基础上扩展了对图执行计划优化的支持，支持了图表一体逻辑执行计划的优化。
* 物理执行计划层
  包含图表的物理执行计划，负责将优化器生成的图表逻辑执行计划通过DAG Builder转换成底层框架可执行的运行时逻辑。


## DSL主要执行流程
DSL的主要执行流程如下图所示：
![DSL执行流程](../../static/img/dsl_workflow.png)

DSL文本首先经过Parser解析生成AST语法树，然后再经过Validator校验器做语义检查和类型推导生成校验后的AST语法树。接着通过Logical Plan转换器生成图表一体的逻辑执行计划。逻辑执行计划通过优化器进行优化处理生成优化后的逻辑执行计划，接下来由物理执行计划转换器转换成物理执行计划，物理执行计划通过DAG Builder生成图表一体的物理执行逻辑。GeaFlow DSL采用有两级DAG结构来描述图表一体的物理执行逻辑。

## 两级DAG物理执行计划
和传统的分布式表数据处理引擎Storm、Flink和Spark的系统不同，GeaFlow是一个流图一体的分布式计算系统。其物理执行计划采用图表两级DAG结构，如下图所示：
![DSl DAG结构](../../static/img/dsl_twice_level_dag.png)
外层DAG包含表处理相关的算子以及图处理的迭代算子，为物理执行逻辑的主体部分，将图表的计算逻辑链接起来。内层DAG则将图计算的逻辑通过DAG方式展开，代表了图迭代计算具体执行方式.

