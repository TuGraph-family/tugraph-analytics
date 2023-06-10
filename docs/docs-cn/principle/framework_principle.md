# Framework原理介绍

## 架构图
GeaFlow Framework的架构如下图所示：

![framework_arch](../../static/img/framework_arch.png)
* GraphView/StreamView API
  API层是对外高阶用户提供的编程开发接口，其提供基于图视图和流视图的语义。
* Unified Execution Graph
  在GeaFlow内部，对于流/图会生成统一的执行计划，物理执行计划由统一的Plan模型构成DAG。
* Unified Cycle Scheduler
  为了让Execution Graph能够在计算引擎上run起来，首先会将ExecutionGraph和ExecutionVertexGroup映射成可调度的Cycle，Cycle可以嵌套。Framework直接针对Cycle通过state machine进行统一的调度执行，同时支持FO容错机制。
* Unified Graph Engine
  这一层是GeaFlow统一的图计算引擎层，支持流/图任务的计算执行。
* Execution Engine
  GeaFlow当前支持运行在云原生(K8S)分布式执行引擎之上。

## 计算引擎
GeaFlow计算引擎核心模块主要包括执行计划生成和优化、统一Cycle调度以及Worker运行时执行。下面就这几个核心模块进行介绍说明。

### 执行计划
针对流图场景提交的PipelineTask，构建统一的执行计划模型，并通过将不同的执行模式，以group方式聚合到一起，为调度提供统一执行单元。

* PipelineGraph
  将用户api提交的PipeleTask构建出PipelineGraph: 将用户api转换为算子后对应一个vertex，veretx之间的数据依赖以edge表示。PipelineGraph只是将api构建为结构化的逻辑执行计划，没有物理执行的语义。

* ExecutionGraph
  将PipelineGraph基于不同的计算模型，将一组可重复执行的vertex聚合到一起，构建对应的ExecutionGroup，每个group表示可以独立调度执行的单元，一个group可以由一个或者多个vertex构建，可以看做一个小的执行计划，group内部数据以pipeline模式交换，group之间数据以batch模式交换。group描述了具体的执行模式，支持嵌套，可以只执行一次，也可只执行多次，可以一次执行一个或者多个窗口的数据。group如下图所示。
  ExecutionGroup最终会转换为调度执行的基本单元cycle。
  ![group.jpg](../../static/img/framework_dag.jpeg)

### 调度模型
调度将基于ExecutionGraph定义的ExecutionGroup生成调度基本单元cycle。cycle描述为可循环执行的基本单元，包含输入，中间计算和数据交换，输出的描述。调度执行过程主要是：
1. 将执行计划划分为一组cycle，cycle与cycle之间如果没有数据依赖，可以串行执行，也可以并行执行。
2. 依据cycle调度数据策略，依次调度执行cycle。
3. 直到所有cycle执行结束，返回调度执行结果。

每个cycle包含一组可被执行的ExecutionTask，每个task可以被分发到远程执行。一个cycle内的所有execution task可以分为如下：
head task: cycle数据流的起点，调度向head task发送一轮执行的event，然后从source，或者上一个cycle输出中读取数据，处理后发送给下游
tail task: cycle数据流的结尾，处理完数据后，向调度发送event，表示完成一轮计算。
其余非head/tail task: 中间执行task，接收上游输入数据，处理后直接发送给下游执行。

cycle调度执行的过程，就是不断发送event给head，并从tail收到返回event的过程，整个过程类似一个“循环”，如下图所示。调度根据不同的cycle类型，初始化调度状态，调度的过程也是状态变迁的过程，根据收到的event，决定下一轮要发送给head的event类型。
![scheduler.jpg](../../static/img/framework_cyle.jpeg)

### Runtime执行
#### 整体介绍
Runtime模块负责GeaFlow所有模式任务（包括流批、静态/动态图）的具体计算和执行。其整个worker流程如下：
1. Scheduler调度器负责将各种类型的Event发送给Container处理
2. Dispatcher(继承至AbstractTaskRunner)负责接收来自Scheduler发送的Event，然后将Event按照其workerId分发给指定的TaskRunner进行处理
3. TaskRunner（也继承至AbstractTaskRunner）负责从taskQueue中获取TASK(Event)，具体Event事件将交由Task进行处理，其整个生命周期包括：创建、处理及结束，对于异常的Task，可以直接中断。
   a. Task创建和初始化会根据CreateTaskEvent事件来完成，Task生命周期结束会根据DestroyTaskEvent事件来完成。
   b. 其它类型的Event，都将通过相应的CommandEvent的execute()来完成具体计算语义层面的逻辑（例如：根据InitCycleEvent事件Worker将进行上下游依赖构建；根据LaunchSourceEvent事件Worker将触发source开始读数据等）
   ![undefined](../../static/img/framework_scheduler.png)

当前Task中的TaskContext核心数据结构，主要包括：负责执行计算的Worker、负责下游节点从上游异步读取数据的FetchService以及负责将执行Worker产生的数据向下游输出的EmitterService。
* Worker：其主要负责流图数据的对齐处理以及将每批处理结束后相应的DoneEvent callback回Scheduler，Scheduler依据相应的DoneEvent进行后续的调度逻辑。
* FetchService：负责异步的从上游channel中Pull数据，并将其通过worker注册的Listener，将数据放入worker processing队列中。
* EmitterService：负责将Worker产生的数据进行partition写入到对应的Channel中。

#### Command Event
* Command Event分为两种：
    * 普通的Command Event，不带具体的execute执行逻辑，通常用于Trigger Task或Cycle进行生命周期的开始和结束；
    * 可执行的Command Event，自身具备execute执行逻辑，例如用于Cycle的初始化、Source节点的数据读取、处理节点的计算、Cycle结束后的清理等等。
* 在调度模块中，会将各种类型的Event构建成一个State Machine，用于整个调度任务的生命周期管理。
* 开发者可以根据设计需要扩展Event以及实现对应的execute计算逻辑，同时加入到State Machine中，Scheduler就可以自动将其按照期望的方式进行调度和执行。

### 容错和异常恢复
#### 集群组件容错
对于运行时的所有组件进程，比如master/driver/container,都基于context初始化和运行。在新创建进程时，首先构建进程需要的context，每个进程在初始化时将context做持久化。当进程异常重启后，首先恢复context，然后基于context重新初始化进程。

#### 作业异常恢复
![undefined](../../static/img/framework_failover.jpeg)
* 作业分布式快照
  调度器根据当前自身调度状态，确定对运行中的任务发送新的windowId，触发对新窗口的计算。当每个算子对应窗口计算结束后，如果需要对当前窗口上下文做快照，则将算子内对应状态持久化到存储中。
  最终调度器收到某个窗口的所有任务执行结束的消息后，也会按需要对该调度元信息做一次快照并持久化，才标志这个窗口的处理最终成。当调度和算子恢复到这个窗口上下文时，则可以基于该窗口继续执行。

* 快照持久化
  在一个window计算完成做一次快照时，可以选择快照存储的方式。目前可选MEMORY，ROCKSDB, JDBC。

* 状态恢复
  快照存储是分布式的，每个组件，调度和算子之间各自存储并持久化。在恢复时，首先调度先从存储中恢复上一次完成执行的windowId，并调度的上下文恢复到对应windowId对应的快照。然后对所有worker发送rollback指令，每个worker恢复到指定窗口上下文。最后由调度开始继续发送执行任务，从恢复状态基础上继续执行。
