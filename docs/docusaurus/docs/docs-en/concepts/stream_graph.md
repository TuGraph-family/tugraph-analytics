## Fundamental Conception

The term "Streaming Graph" refers to graph data that is stream-based, dynamic, and constantly changing. Within the context of GeaFlow, Streaming Graph also refers to the computing mode for streaming graphs, Which is designed for graphs that undergo streaming changes, and performs operations such as graph traversal, graph matching, and graph computation based on graph changes.


Based on the GeaFlow framework, it is easy to perform dynamic computation on streaming graphs. In GeaFlow, we have abstracted two core concepts: Dynamic Graph and Static Graph.

* Static Graph refers to a static graph, in which the nodes and edges are fixed at a certain point in time and do not change. Computation on Static Graph is based on the static structure of the entire graph, so conventional graph algorithms and processing can be used for computation.

* Dynamic Graph refers to a dynamic graph, where nodes and edges are constantly changing. When the status of a node or edge changes, Dynamic Graph updates the graph structure promptly and performs computation on the new graph structure. In Dynamic Graph, nodes and edges can have dynamic attributes, which can also change with the graph. Computation on Dynamic Graph is based on the real-time structure and attributes of the graph, so special algorithms and processing are required for computation.

GeaFlow provides various computation modes and algorithms based on Dynamic Graph and Static Graph to facilitate users' choices and usage based on different needs. At the same time, GeaFlow also supports custom algorithms and processing, so users can extend and optimize algorithms according to their own needs.

## Functional Description

Streaming Graph mainly has the following features:

* Supports streaming processing of node and edge data, but the overall graph is static.
* Supports continuous updates and queries of the graph structure, and can handle incremental data processing caused by changes in the graph structure.
* Supports backtracking history and can be queried based on historical snapshots.
* Supports the calculation logic order of the graph, such as the time sequence of edges.

Through real-time graph data flow and changes, Streaming Graph can dynamically implement graph calculations and analysis, and has a wide range of applications. For example, in the fields of social network analysis, financial risk control, and Internet of Things data analysis, Streaming Graph has broad applications prospects.


## Example Introduction

When building a Streaming Graph, a new node and edge can be added to the graph continuously through an incremental data stream, thus dynamically constructing the graph. At the same time, for each incremental data graph construction completion, it can trigger traversal calculation tracking the evolving process of Bob's 2-degree friends over time.

DSL code
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

HLA code

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