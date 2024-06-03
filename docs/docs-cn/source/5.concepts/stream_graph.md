# Stream Graph

## 基本概念

Streaming Graph指的是流式、动态、变化的图数据，同时在GeaFlow内部Streaming Graph也指对流式图的计算模式，它针对流式变化的图，基于图的变化进行图遍历、图匹配和图计算等操作。

基于GeaFlow框架，可以方便的针对流式变化的图动态计算。在GeaFlow中，我们抽象了Dynamic Graph和Static Graph两个核心的概念。
* Dynamic Graph 是指流式变化的图，它是图在时间轴上不断变化的切片所组成，可以方便的研究图随着时间推移的演化过程。
* Static Graph 是图在某个时间点的 Snapshot，相当于 Dynamic Graph 的一个时间切片。

## 功能描述


Streaming Graph 主要有以下几个功能：

* 支持流式地处理点、边数据，支持在最新的图做查询。
* 支持持续不断的更新和查询图结构，支持图结构变化带来的增量数据处理。
* 支持回溯历史，基于历史快照做查询。
* 支持图计算的计算逻辑顺序，例如基于边的时间序做计算。


## 示例介绍

读取点、边两个无限数据流增量构图，对于每次增量数据构图完成，会触发 traversal 计算，查找 'Bob' 的2度内的朋友随着时间推移的演进过程。

DSL 代码
```SQL

set geaflow.dsl.window.size = 1;

CREATE TABLE table_knows (
  personId int,
  friendId int,
  weight int
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/table_knows.txt'
);

INSERT INTO social_network.knows
SELECT personId, friendId, weight
FROM table_knows;

CREATE TABLE result (
  personName varchar,
  friendName varchar,
  weight int
) WITH (
	type='console'
);

-- Graph View Name Defined in Graph View Concept --
USE GRAPH social_network;
-- find person id 3's known persons triggered every window.
INSERT INTO result
SELECT
	name,
	known_name,
	weight
FROM (
  MATCH (a:person where a.name = 'Bob') -[e:knows]->{1, 2}(b)
  RETURN a.name as name, b.name as known_name, e.weight as weight
)
```

HLA 代码

```java 
//build graph view.
final String graphName = "social_network";
GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName).build();
pipeline.withView(graphName, graphViewDesc);

// submit pipeLine task.
pipeline.submit(new PipelineTask() {
	@Override
	public void execute(IPipelineTaskContext pipelineTaskCxt) {

        // build vertices streaming source.
		PStreamSource<IVertex<Integer, String>> persons =
			pipelineTaskCxt.buildSource(
				new CollectionSource.(getVertices()), SizeTumblingWindow.of(5000));
		// build edges streaming source.
		PStreamSource<IEdge<Integer, Integer>> knows =
			pipelineTaskCxt.buildSource(
				new CollectionSource<>(getEdges()), SizeTumblingWindow.of(5000));
		// build graphview by graph name.
		PGraphView<Integer, String, Integer> socialNetwork =
			pipelineTaskCxt.buildGraphView(graphName);
		// incremental build graph view.
		PIncGraphView<Integer, String, Integer> incSocialNetwor =
			socialNetwork.appendGraph(vertices, edges);

		// traversal by 'Bob'.
		incGraphView.incrementalTraversal(new IncGraphTraversalAlgorithms(2))
			.start('Bob')
			.map(res -> String.format("%s,%s", res.getResponseId(), res.getResponse()))
			.sink(new ConsoleSink<>());
	}
});
```