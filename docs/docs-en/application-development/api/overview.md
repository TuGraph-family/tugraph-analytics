# API Introduction
GeaFlow API is a development interface provided for advanced users, which supports two types of APIs: Graph API and Stream API:
* Graph API: Graph is a first-class citizen of the GeaFlow framework. Currently, the GeaFlow framework provides a set of graph computing programming interfaces based on GraphView, including graph construction, graph computation, and traversal. In GeaFlow, both static and dynamic graphs are supported.
    * Static Graph API: Static graph computing API. Full graph computing or traversal can be performed based on this API.
    * Dynamic Graph API: Dynamic graph computing API. GraphView is the data abstraction of a dynamic graph in GeaFlow. Based on GraphView, dynamic graph computing or traversal can be performed. At the same time, support for Snapshot generation from GraphView is provided. The Snapshot can provide the same interface capability as the Static Graph API.
      ![api_arch](../../../static/img/api_arch.jpeg)
* The Stream API: GeaFlow provides a set of programming interfaces for general computing, including source construction, streaming batch computing, and sink output. In GeaFlow, both Batch and Stream are supported.
    * Batch API: Batch computing API, which can perform batch computation based on this API.
    * Stream API: Streaming computing API. StreamView is the data abstraction of a dynamic stream in GeaFlow. Based on StreamView, streaming computing can be performed.

From the introduction of the two types of APIs, it can be seen that GeaFlow unifies the graph view and stream view semantics through View internally. At the same time, in order to support two sets of APIs for dynamic and static computing, GeaFlow abstracts the concept of Window internally. Starting from the Source API, a Window must be included to split data windows based on the Window.
* For streaming or dynamic graph APIs, the Window can be split by size, and each window reads a certain size of data to achieve incremental computation.
* For batch or static graph APIs, the Window will use the AllWindow mode, and a window will read the full amount of data to achieve full computation.

# Maven依赖
To develop a GeaFlow API application, you need to add maven dependencies:

```xml
<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-api</artifactId>
    <version>0.1</version>
</dependency>

<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-pdata</artifactId>
    <version>0.1</version>
</dependency>

<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-cluster</artifactId>
    <version>0.1</version>
</dependency>

<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-on-local</artifactId>
    <version>0.1</version>
</dependency>

<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-pipeline</artifactId>
    <version>0.1</version>
</dependency>
```

# Overview Of Functions
## Graph API
Graph API is a first-class citizen in GeaFlow, which provides a set of graph computing programming interfaces based on GraphView, including graph construction, graph computation, and traversal. The specific API descriptions are shown in the following table:
<table>
	<tr>
		<td>Type</td>
		<td>API</td>
		<td>Explanation</td>
	</tr>
	<tr>
		<td rowspan="7">Dynamic Graph</td>
		<td>PGraphView<K, VV, EV> init(GraphViewDesc graphViewDesc)</td>
		<td>Initialize with graphViewDesc as input</td>
	</tr>
	<tr>
		<td>PGraphView<K, VV, EV> PIncGraphView<K, VV, EV> appendVertex(PWindowStream<IVertex<K, VV>> vertexStream)     </td>
		<td>Using a distributed vertex stream as the incremental set of graph vertices for GraphView</td>
	</tr>
	<tr>
		<td>PIncGraphView<K, VV, EV> appendEdge(PWindowStream<IEdge<K, EV>> edgeStream)     </td>
		<td>Using a distributed edge stream as the incremental set of graph edges for GraphView</td>
	</tr>
	<tr>
		<td>PIncGraphView<K, VV, EV> appendGraph(PWindowStream<IVertex<K, VV>> vertexStream, PWindowStream<IEdge<K, EV>> edgeStream)     </td>
		<td>Using a distributed vertex and edge stream as the incremental set of graph vertices/edges for GraphView</td>
	</tr>
	<tr>
		<td><M, R> PGraphTraversal<K, R> incrementalTraversal(IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal)     </td>
		<td>Performing incremental graph traversal on a dynamic GraphView</td>
	</tr>
	<tr>
		<td>PGraphCompute<K, VV, EV> incrementalCompute(IncVertexCentricCompute<K, VV, EV, M> incVertexCentricCompute)     </td>
		<td>Performing incremental graph computation on a dynamic GraphView</td>
	</tr>
	<tr>
		<td>void materialize()     </td>
		<td>Storing the incremental set of points and edges from a dynamic GraphView into state </td>
	</tr>
	<tr>
		<td rowspan="7">Static Graph</td>
		<td><M> PGraphCompute<K, VV, EV> compute(VertexCentricCompute<K, VV, EV, M> vertexCentricCompute)</td>
		<td>Performing static graph vertex centric computation on Graph</td>
	</tr>
	<tr>
		<td><UPDATE> PGraphWindow<K, VV, EV> compute(ScatterGatherCompute<K, VV, EV, UPDATE> sgAlgorithm, int parallelism)     </td>
		<td>Performing static graph scatter gather computation on Graph</td>
	</tr>
	<tr>
		<td><M, R> PGraphTraversal<K, R> traversal(VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal)     </td>
		<td>Performing static graph vertex centric traversal on Graph</td>
	</tr>
	<tr>
		<td><M, R> PWindowStream<IEdge<K, EV>> getEdges()     </td>
		<td>Return a collection of edges</td>
	</tr>
	<tr>
		<td>PWindowStream<IVertex<K, VV>> getVertices()     </td>
		<td>Return a collection of vertices </td>
	</tr>
</table>

## Stream API
The Stream API provides a set of programming interfaces for general computation, including source construction, stream and batch computing, and sink output. The specific API documentation is shown in the table below:
<table>
	<tr>
		<td>Type</td>
		<td>API</td>
		<td>Explanation</td>
	</tr>
	<tr>
		<td rowspan="4">Stream</td>
		<td>PStreamView<T> init(IViewDesc viewDesc)</td>
		<td>Initializing with a StreamViewDesc</td>
	</tr>
	<tr>
		<td>PIncStreamView<T> append(PWindowStream<T> windowStream)     </td>
		<td>Using distributed data as an incremental dataset for a StreamView</td>
	</tr>
	<tr>
		<td>PWindowStream<T> reduce(ReduceFunction<T> reduceFunction)     </td>
		<td>Performing incremental reduce aggregation on a dynamic StreamView</td>
	</tr>
	<tr>
		<td><ACC, OUT> PWindowStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction)     </td>
		<td>Performing incremental aggregate aggregation on a dynamic StreamView</td>
	</tr>
	<tr>
		<td rowspan="12">Batch</td>
		<td>PStreamView<T> <R> PWindowStream<R> map(MapFunction<T, R> mapFunction)</td>
		<td>Performing map operation</td>
	</tr>
	<tr>
		<td>PWindowStream<T> filter(FilterFunction<T> filterFunction)     </td>
		<td>Performing filter operation</td>
	</tr>
	<tr>
		<td><R> PWindowStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction)      </td>
		<td>Performing flatMap operation</td>
	</tr>
	<tr>
		<td><ACC, OUT> PWindowStream<T> union(PStream<T> uStream)     </td>
		<td>Performing union merge on two streams</td>
	</tr>
	<tr>
		<td>PWindowBroadcastStream<T> broadcast()     </td>
		<td>Broadcasting a stream downstream</td>
	</tr>
	<tr>
		<td><KEY> PWindowKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction)     </td>
		<td>Performing keyby operation based on selectorFunction rules</td>
	</tr>
	<tr>
		<td>PStreamSink<T> sink(SinkFunction<T> sinkFunction)     </td>
		<td>Outputting the results</td>
	</tr>
	<tr>
		<td>PWindowCollect<T> collect()     </td>
		<td>Triggering the collection of data results</td>
	</tr>
	<tr>
		<td>PWindowStream<T> reduce(ReduceFunction<T> reduceFunction)     </td>
		<td>Performing a reduce aggregation calculation within a window</td>
	</tr>
	<tr>
		<td><ACC, OUT> PWindowStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction)     </td>
		<td>Performing a aggregate aggregation calculation within a window</td>
	</tr>
	<tr>
		<td><ACC, OUT> PIncStreamView<T> materialize()     </td>
		<td>Using a PWindowKeyStream as a dynamic StreamView and generating an IncStreamView after default keyby</td>
	</tr>
</table>




# Typical Example
## Introduction to PageRank dynamic graph computing example
### The Definition Of PageRank
PageRank algorithm was originally used to calculate the importance of Internet pages. It was proposed by Page and Brin in 1996 and used in the ranking of web pages in Google search engine. In fact, PageRank can be defined on any digraph and later applied to many problems such as social influence analysis, text summary and so on.
Assuming that the Internet is a directed graph, the random walk model is defined on the basis of which, namely, the first-order Markov chain, represents the process of web page visitors browsing the web pages randomly on the Internet. It is assumed that the viewer jumps to the next page with equal probability according to the hyperlink connected out in each page, and continues to carry out such a random jump on the Internet, this process forms a first-order Markov chain. PageRank represents the smooth distribution of this Markov chain. The PageRank value of each page is the stationary probability.
The implementation of the algorithm is as follows: 1. Assume that the initial influence value of each point in the figure is the same; 2. 2. Calculate the jump probability of each point to other points, and update the influence value of the point; 3. Perform n iterations until the influence value of each point no longer changes, that is the convergence state.

### Example Code

```java

public class IncrGraphCompute {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrGraphCompute.class);
	// Computed result path.
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/incr_graph";
	// Comparison result path.
    public static final String REF_FILE_PATH = "data/reference/incr_graph";

    public static void main(String[] args) {
		// Get execution environment.
        Environment environment = EnvironmentFactory.onLocalEnvironment();
		// Perform job submission.
        IPipelineResult result = submit(environment);
		// Wait for execution to complete.
        result.get();
		// Close the execution environment.
		environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
		// Build the task execution flow.
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
		// Get the job environment configuration.
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
		// Specifies the path to save the calculation results.
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);

        // Graphview name.
        final String graphName = "graph_view_name";
		// Create delta graph graphview.
        GraphViewDesc graphViewDesc = GraphViewBuilder
			.createGraphView(graphName)
			// Set the number of graphview shards, which can be specified from the configuration.
            .withShardNum(envConfig.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM))
			// Set graphview backend type.
            .withBackend(BackendType.RocksDB)
			// Specify graphview schema information such as vertices/edges and attributes.
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class, Integer.class, ValueEdge.class, IntegerType.class))
            .build();
		// Add the created graphview information to the task execution flow.
        pipeline.withView(graphName, graphViewDesc);
		
		// Submit the task and execute.
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
				// 1. Build vertex data input source.
                PWindowSource<IVertex<Integer, Integer>> vertices =
                    // Extract vertex from edge file.
                    pipelineTaskCxt.buildSource(new RecoverableFileSource<>("data/input/email_edge",			
						// Specifies the parsing format for each row of data.
                        line -> {
                            String[] fields = line.split(",");
                            IVertex<Integer, Integer> vertex1 = new ValueVertex<>(
                                Integer.valueOf(fields[0]), 1);
                            IVertex<Integer, Integer> vertex2 = new ValueVertex<>(
                                Integer.valueOf(fields[1]), 1);
                            return Arrays.asList(vertex1, vertex2);
                        }), SizeTumblingWindow.of(10000))
						// Specifies the parallelism of vertex data sources.
                        .withParallelism(pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));
				
				// 2. Build the edge data input source.
                PWindowSource<IEdge<Integer, Integer>> edges =
                    pipelineTaskCxt.buildSource( new RecoverableFileSource<>("data/input/email_edge",			
						// Specifies the parsing format for each row of data.
                        line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), SizeTumblingWindow.of(5000))
						// Specifies the parallelism of edge data sources.
						.withParallelism(pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));
				
				// Get the defined graphview and build the graph data.
                PGraphView<Integer, Integer, Integer> fundGraphView =
                    pipelineTaskCxt.getGraphView(graphName);
                PIncGraphView<Integer, Integer, Integer> incGraphView =
                    fundGraphView.appendGraph(vertices, edges);
				// Get the concurrency of Map tasks running in a job.
                int mapParallelism = pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.MAP_PARALLELISM);
				// Get the concurrency of Sink tasks running in a job.
                int sinkParallelism = pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.SINK_PARALLELISM);
				// Create sink method.
                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);			
				// Based on graph algorithm, dynamic graph calculation is performed.
                incGraphView.incrementalCompute(new IncGraphAlgorithms(3))
					// Get the result of vertices data and perform a map operation.
                    .getVertices()
                    .map(v -> String.format("%s,%s", v.getId(), v.getValue()))
                    .withParallelism(mapParallelism)
                    .sink(sink)
                    .withParallelism(sinkParallelism);
            }
        });
		
        return pipeline.execute();
    }
	
	// Implement Pagerank dynamic graph algorithm.
    public static class IncGraphAlgorithms extends IncVertexCentricCompute<Integer, Integer,
        Integer, Integer> {

        public IncGraphAlgorithms(long iterations) {
			// Set the maximum number of iterations for the algorithm.
            super(iterations);
        }

        @Override
        public IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> getIncComputeFunction() {
			// Specify the Pagerank calculation logic.
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class PRVertexCentricComputeFunction implements
        IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> {

        private IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext;
		
		// Init method, set graphContext.
        @Override
        public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext) {	
            this.graphContext = graphContext;
        }
		
		// The first round of iteration implementation of the evolve method.
        @Override
        public void evolve(Integer vertexId,
                           TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
			// Set the dynamic graph version to 0.
            long lastVersionId = 0L;
			// Get the vertex whose ID is equal to vertexId from the incremental graph.
            IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
			// Get the historical base graph.
            HistoricalGraph<Integer, Integer, Integer> historicalGraph = graphContext
                .getHistoricalGraph();
            if (vertex == null) {
				// If there is no vertex with ID equal to vertexId in the incremental graph, get it from the historical graph.
                vertex = historicalGraph.getSnapShot(lastVersionId).vertex().get();
            }

            if (vertex != null) {
				// Get all the outgoing edges corresponding to a vertex from the incremental graph.
                List<IEdge<Integer, Integer>> newEs = temporaryGraph.getEdges();
				// Get all the outgoing edges corresponding to a vertex from the historical graph.
                List<IEdge<Integer, Integer>> oldEs = historicalGraph.getSnapShot(lastVersionId)
                    .edges().getOutEdges();
                if (newEs != null) {
                    for (IEdge<Integer, Integer> edge : newEs) {
						// Send a message with the content of vertexId to the targetId of all edges in the incremental graph.
                        graphContext.sendMessage(edge.getTargetId(), vertexId);
                    }
                }
                if (oldEs != null) {
                    for (IEdge<Integer, Integer> edge : oldEs) {
						// Send a message with the content of vertexId to the targetId of all edges in the historical graph.
                        graphContext.sendMessage(edge.getTargetId(), vertexId);
                    }
                }
            }

        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            int max = 0;
			// Iterate through all messages received by vertexId and take the maximum value.
            while (messageIterator.hasNext()) {
                int value = messageIterator.next();
                max = max > value ? max : value;
            }
			// Get the vertex whose ID is equal to vertexId from the incremental graph.
            IVertex<Integer, Integer> vertex = graphContext.getTemporaryGraph().getVertex();		
			// Get the vertex whose ID is equal to vertexId from the historical graph.
            IVertex<Integer, Integer> historyVertex = graphContext.getHistoricalGraph().getSnapShot(0).vertex().get();
			// Take the maximum value between the attribute value of a vertex in the incremental graph and the maximum value of the messages.
            if (vertex != null && max < vertex.getValue()) {
                max = vertex.getValue();
            }
			// Take the maximum value between the attribute value of a vertex in the historical graph and the maximum value of the messages.
            if (historyVertex != null && max < historyVertex.getValue()) {
                max = historyVertex.getValue();
            }
			// Update the attribute value of a vertex in the incremental graph.
            graphContext.getTemporaryGraph().updateVertexValue(max);
        }
		
		
        @Override
        public void finish(Integer vertexId, MutableGraph<Integer, Integer, Integer> mutableGraph) {
			// Get the vertices and edges related to vertexId from the incremental graph.
            IVertex<Integer, Integer> vertex = graphContext.getTemporaryGraph().getVertex();
            List<IEdge<Integer, Integer>> edges = graphContext.getTemporaryGraph().getEdges();
            if (vertex != null) {
				// Add the vertices in the incremental graph to the graph data.
                mutableGraph.addVertex(0, vertex);
                graphContext.collect(vertex);
            } else {
                LOGGER.info("not found vertex {} in temporaryGraph ", vertexId);
            }
            if (edges != null) {
				// Add the edges in the incremental graph to the graph data.
                edges.stream().forEach(edge -> {
                    mutableGraph.addEdge(0, edge);
                });
            }
        }
    }

}

```

## Introduction to PageRank static graph computing example
### Example Code

```java

public class PageRank {

    private static final Logger LOGGER = LoggerFactory.getLogger(PageRank.class);
	
	// Computed result path.
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/pagerank";
	
	// Comparison result path.
    public static final String REF_FILE_PATH = "data/reference/pagerank";

    public static void main(String[] args) {
		// Get execution environment.
        Environment environment = EnvironmentFactory.onLocalEnvironment();
		// Perform job submission.
        IPipelineResult<?> result = submit(environment);
		// Wait for execution to complete.
        result.get();
		// Close the execution environment.
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
		// Build the task execution flow.
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
		// Get the job environment configuration.
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
		// Specifies the path to save the calculation results.
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
		
		// Submit the task and execute.
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
			// 1. Build vertex data input source.
            PWindowSource<IVertex<Integer, Double>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
					// Specifies the parsing format for each row of data.
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Double> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Double.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
					// Specifies the parallelism of vertex data sources.
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));
			// 2. Build the edge data input source.
            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
				// Specifies the parsing format for each row of data.
                line -> {
                    String[] fields = line.split(",");
                    IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                    return Collections.singletonList(edge);
                }), AllWindow.getInstance())
				// Specifies the parallelism of edge data sources.
                .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));
			
			// The parallelism of iteration computation.
            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);		
			// Define graphview.
            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
				// Specify the number of shards as 2.
                .withShardNum(2)
				// Specify the backend type as memory.
                .withBackend(BackendType.Memory)
                .build();
			
			// Build a static graph based on vertex/edge data and the defined graph view.
            PGraphWindow<Integer, Double, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);
			// Create sink method.
            SinkFunction<IVertex<Integer, Double>> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
			// Specify the computation concurrency and execute the static computation method.
            graphWindow.compute(new PRAlgorithms(3))
                .compute(iterationParallelism)
				// Get the computed vertices result and output them according to the defined sink function.
                .getVertices()
                .sink(sink)
                .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });

        return pipeline.execute();
    }

    public static class PRAlgorithms extends VertexCentricCompute<Integer, Double, Integer, Double> {

        public PRAlgorithms(long iterations) {
			// Specify the iteration number for static graph computation.
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Integer, Double, Integer, Double> getComputeFunction() {
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }

    }

    public static class PRVertexCentricComputeFunction extends AbstractVcFunc<Integer, Double, Integer, Double> {
		// The implementation of the static graph computation method.
        @Override
        public void compute(Integer vertexId,
                            Iterator<Double> messageIterator) {
			// Get the vertex from the static graph where the vertex ID equals vertexId.
            IVertex<Integer, Double> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1) {
				// In the first iteration, send messages to neighboring nodes, and the message content is the attribute value of the vertex with vertexId.
                this.context.sendMessageToNeighbors(vertex.getValue());
            } else {
                double sum = 0;
                while (messageIterator.hasNext()) {
                    double value = messageIterator.next();
                    sum += value;
                }
				// Sum up the received messages and set it as the attribute value of the current vertex.
                this.context.setNewVertexValue(sum);
            }
        }

    }
}

```

## Introduction to WordCount batch computation example
### Example Code

```java

public class WordCountStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStream.class);
	
	// Computed result path.
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/wordcount";
	
	// Comparison result path.
    public static final String REF_FILE_PATH = "data/reference/wordcount";

    public static void main(String[] args) {
		// Get execution environment.
        Environment environment = EnvironmentUtil.loadEnvironment(args);
		// Perform job submission.
        IPipelineResult<?> result = submit(environment);
        result.get();
		// Close the execution environment.
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
		// Build the task execution flow.
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
		// Get the job environment configuration.
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
		// Turn off the materialize switch for dynamic streaming.
        envConfig.getConfigMap().put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE.getKey(), Boolean.TRUE.toString());
		// Specifies the path to save the calculation results.
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);

        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();

				// Gets the input data stream.
                PWindowSource<String> streamSource = pipelineTaskCxt.buildSource(
                    new FileSource<String>("data/input/email_edge",
						// Specifies the parsing format for each row of data.
                        line -> {
                            String[] fields = line.split(",");
                            return Collections.singletonList(fields[0]);
								// Define the size of the data window.
                        }) {}, SizeTumblingWindow.of(5000))
					// Specifies the parallelism of input data sources.
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);			
                streamSource
					// Perform map operation on the data in the stream.
                    .map(e -> Tuple.of(e, 1))
					// Key by.
                    .keyBy(new KeySelectorFunc())
					// Reduce: the aggregation to count the number of identical data.
                    .reduce(new CountFunc())
					// Specify the concurrency num of the operator.
                    .withParallelism(conf.getInteger(ExampleConfigKeys.REDUCE_PARALLELISM))
                    .map(v -> String.format("(%s,%s)", ((Tuple) v).f0, ((Tuple) v).f1))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
            }
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_PATH, RESULT_FILE_PATH);
    }


    public static class MapFunc implements MapFunction<String, Tuple<String, Integer>> {
		// Implement the map method to convert each input word to a Tuple.
        @Override
        public Tuple<String, Integer> map(String value) {
            LOGGER.info("MapFunc process value: {}", value);
            return Tuple.of(value, 1);
        }
    }

    public static class KeySelectorFunc implements KeySelector<Tuple<String, Integer>, Object> {

        @Override
        public Object getKey(Tuple<String, Integer> value) {
            return value.f0;
        }
    }

    public static class CountFunc implements ReduceFunction<Tuple<String, Integer>> {

        @Override
        public Tuple<String, Integer> reduce(Tuple<String, Integer> oldValue, Tuple<String, Integer> newValue) {
            return Tuple.of(oldValue.f0, oldValue.f1 + newValue.f1);
        }
    }
}

```


## Introduction to KeyAgg stream computation example
### Example Code

```java

public class WindowStreamKeyAgg implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowStreamKeyAgg.class);
	// Computed result path.
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/wordcount";
	
	// Comparison result path.
    public static final String REF_FILE_PATH = "data/reference/wordcount";

    public static void main(String[] args) {
		// Get execution environment.
        Environment environment = EnvironmentUtil.loadEnvironment(args);
		// Perform job submission.
        IPipelineResult<?> result = submit(environment);
        result.get();
		// Close the execution environment.
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
		// Build the task execution flow.
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
		// Get the job environment configuration.
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
		// Specifies the path to save the calculation results.
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();

				// Define the window size as 5000 and construct an input data stream.
                PWindowSource<String> streamSource =
                    pipelineTaskCxt.buildSource(new FileSource<String>("data/input"
                    + "/email_edge", Collections::singletonList) {}, SizeTumblingWindow.of(5000));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    .flatMap(new FlatMapFunction<String, Long>() {
                        @Override
                        public void flatMap(String value, Collector collector) {
							// Flatmap implementation.
                            String[] records = value.split(SPLIT);
                            for (String record : records) {
								// Split and partition each line of data.
                                collector.partition(Long.valueOf(record));
                            }
                        }
                    })
					// Map.
                    .map(p -> Tuple.of(p, p))
					// Key by.
                    .keyBy(p -> ((long) ((Tuple) p).f0) % 7)
                    .aggregate(new AggFunc())
                    .withParallelism(conf.getInteger(AGG_PARALLELISM))
                    .map(v -> String.format("%s,%s", ((Tuple) v).f0, ((Tuple) v).f1))
                    .sink(sink).withParallelism(conf.getInteger(SINK_PARALLELISM));
            }
        });

        return pipeline.execute();
    }

	
    public static class AggFunc implements
        AggregateFunction<Tuple<Long, Long>, Tuple<Long, Long>, Tuple<Long, Long>> {
		
		// Define the accumulator implementation.
        @Override
        public Tuple<Long, Long> createAccumulator() {
            return Tuple.of(0L, 0L);
        }
		
        @Override
        public void add(Tuple<Long, Long> value, Tuple<Long, Long> accumulator) {
			// Add up the f1 values of two values with the same key.
            accumulator.setF0(value.f0);
            accumulator.setF1(value.f1 + accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> getResult(Tuple<Long, Long> accumulator) {
			// Return the result after accumulation.
            return Tuple.of(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> merge(Tuple<Long, Long> a, Tuple<Long, Long> b) {
            return null;
        }
    }

}


```