# Graph Traversal Introduction
GeaFlow provides interfaces for implementing graph traversal algorithms, which can be used for subgraph traversal and full graph traversal. Users can choose to continue traversing vertices or edges in the traversal algorithm and define the number of iterations.

# Dynamic Graph
## Interface
| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| void open(IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> vertexCentricFuncContext) | Perform the open operation of vertexCentricFunction | vertexCentricFuncContext: where K represents the type of vertex ID, VV represents the type of vertex value, EV represents the type of edge value, M represents the type of message defined in graph traversal, and R represents the type of traversal result |
| void init(ITraversalRequest traversalRequest) | Graph traversal initialization interface | traversalRequest: Trigger vertices for graph traversal, where K represents the type of vertex ID |
| void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph) | Implement processing logic for incremental graph during the first round of computation | vertexId: ID of the current computing vertex, where K represents the type of vertex ID. <br>temporaryGraph: Temporary incremental graph, where K represents the type of vertex ID, VV represents the type of vertex value, and EV represents the type of edge value |
| void compute(K vertexId, Iterator messageIterator) | Graph traversal interface | vertexId: ID of the current computing vertex, where K represents the type of vertex ID. <br>messageIterator: All messages sent to the current vertex during graph traversal, where M represents the type of message defined in the traversal iteration process |
| void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph) | Graph traversal complete interface | vertexId: ID of the current computing vertex, where K represents the type of vertex ID. <br>mutableGraph: Mutable graph, where K represents the type of vertex ID, VV represents the type of vertex value, and EV represents the type of edge value |

* Detailed interface
```java
   public interface IncVertexCentricTraversalFunction<K, VV, EV, M, R> extends IncVertexCentricFunction<K, VV
   , EV, M> {

   void open(IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> vertexCentricFuncContext);

   void init(ITraversalRequest<K> traversalRequest);

   void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph);

   void compute(K vertexId, Iterator<M> messageIterator);

   void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph);

   interface IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> extends IncGraphContext<K, VV, EV,
   M> {
   /** Activate traversal starting point for use in the following iteration. */
   void activeRequest(ITraversalRequest<K> request);
   /** Collect traversal results. */
   void takeResponse(ITraversalResponse<R> response);

        void broadcast(IGraphMessage<K, M> message);
    	/** Get historical graph data. */
        TraversalHistoricalGraph<K, VV, EV> getHistoricalGraph();
   }


    interface TraversalHistoricalGraph<K, VV, EV>  extends HistoricalGraph<K, VV, EV> {
    	/** Get the snapshot of specified version. */
        TraversalGraphSnapShot<K, VV, EV> getSnapShot(long version);
    }

    interface TraversalGraphSnapShot<K, VV, EV> extends GraphSnapShot<K, VV, EV> {
    	/** Get the starting vertex for graph traversal. */
        TraversalVertexQuery<K, VV> vertex();
    	/** Get the starting edge for graph traversal. */
        TraversalEdgeQuery<K, EV> edges();
    }
}
```

## Example
```java
public class IncrGraphTraversalAll {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(IncrGraphTraversalAll.class);
    
    public static void main(String[] args) {
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(2)
            .withBackend(BackendType.RocksDB)
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class, Integer.class, ValueEdge.class, IntegerType.class))
            .build();
        pipeline.withView(graphName, graphViewDesc);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                PWindowSource<IVertex<Integer, Integer>> vertices =
                    pipelineTaskCxt.buildSource(new RecoverableFileSource<>("data/input/email_edge",
                            line -> {
                                String[] fields = line.split(",");
                                IVertex<Integer, Integer> vertex1 = new ValueVertex<>(
                                    Integer.valueOf(fields[0]), 1);
                                IVertex<Integer, Integer> vertex2 = new ValueVertex<>(
                                    Integer.valueOf(fields[1]), 1);
                                return Arrays.asList(vertex1, vertex2);
                            }), SizeTumblingWindow.of(10000));
                
                PWindowSource<IEdge<Integer, Integer>> edges =
                    pipelineTaskCxt.buildSource( new RecoverableFileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), SizeTumblingWindow.of(5000));

                PGraphView<Integer, Integer, Integer> fundGraphView =
                    pipelineTaskCxt.getGraphView(graphName);
                PIncGraphView<Integer, Integer, Integer> incGraphView =
                    fundGraphView.appendGraph(vertices, edges);
                incGraphView.incrementalTraversal(new IncGraphTraversalAlgorithms(3))
                    .start()
                    .sink(v -> {});
            }
        });
        IPipelineResult result = pipeline.execute();
        result.get();
    }
    
    public static class IncGraphTraversalAlgorithms extends IncVertexCentricTraversal<Integer,
            Integer, Integer, Integer, Integer> {
        
        public IncGraphTraversalAlgorithms(long iterations) {
            super(iterations);
        }
        
        @Override
        public IncVertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer> getIncTraversalFunction() {
            return new IncVertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer>() {

                private IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer, Integer> vertexCentricFuncContext;

                @Override
                public void open(IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer,
                    Integer> vertexCentricFuncContext) {
                    this.vertexCentricFuncContext = vertexCentricFuncContext;
                }

                @Override
                public void evolve(Integer vertexId,
                                   TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
                    MutableGraph<Integer, Integer,
                        Integer> mutableGraph = this.vertexCentricFuncContext.getMutableGraph();
                    IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
                    if (vertex != null) {
                        mutableGraph.addVertex(0, vertex);
                    }
                    List<IEdge<Integer, Integer>> edges = temporaryGraph.getEdges();
                    if (edges != null) {
                        for (IEdge<Integer, Integer> edge : edges) {
                            mutableGraph.addEdge(0, edge);
                        }
                    }
                }

                @Override
                public void init(ITraversalRequest<Integer> traversalRequest) {
                    int requestId = traversalRequest.getVId();
                    List<IEdge<Integer, Integer>> edges =
                        this.vertexCentricFuncContext.getHistoricalGraph().getSnapShot(0).edges().getEdges();
                    int sum = 0;
                    if (edges != null) {
                        for (IEdge<Integer, Integer> edge : edges) {
                            sum += edge.getValue();
                        }
                    }
                    this.vertexCentricFuncContext.takeResponse(new TraversalResponse(requestId, sum));
                }

                @Override
                public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
                }

                @Override
                public void finish(Integer vertexId,
                                   MutableGraph<Integer, Integer, Integer> mutableGraph) {
                }
            };
        }
        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }
    }

    static class TraversalResponse implements ITraversalResponse<Integer> {

        private long responseId;

        private int value;

        public TraversalResponse(long responseId, int value) {
            this.responseId = responseId;
            this.value = value;
        }
        @Override
        public long getResponseId() {
            return responseId;
        }
        @Override
        public Integer getResponse() {
            return value;
        }
        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }
        @Override
        public String toString() {
            return responseId + "," + value;
        }
    }
}
```

# Statical Graph

## Interface
| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| void open(VertexCentricTraversalFuncContext<K, VV, EV, M, R> vertexCentricFuncContext) | Perform open operation using vertexCentric function | vertexCentricFuncContext: K represents the type of vertex ID, VV represents the type of vertex value, EV represents the type of edge value, M represents the type of message defined in graph traversal, and R represents the type of traversal result |
| void init(ITraversalRequest traversalRequest) | Graph traversal initialization interface | Traversal request: Graph traversal trigger vertex, where K represents the type of vertex ID |
| void compute(K vertexId, Iterator messageIterator) | Graph traversal interface | vertexId: ID of the current computing vertex, where K represents the type of vertex ID. <br>messageIterator: All messages sent to the current vertex during graph traversal, where M represents the type of message defined in the traversal iteration process |


* Detailed interface
```java
public interface VertexCentricTraversalFunction<K, VV, EV, M, R> extends VertexCentricFunction<K, VV
    , EV, M> {

    void open(VertexCentricTraversalFuncContext<K, VV, EV, M, R> vertexCentricFuncContext);
	/** Graph traversal algorithm initialization method. */
    void init(ITraversalRequest<K> traversalRequest);
	/** Implement graph traversal logic. */
    void compute(K vertexId, Iterator<M> messageIterator);

    void finish();

    void close();
	
    interface VertexCentricTraversalFuncContext<K, VV, EV, M, R> extends VertexCentricFuncContext<K,
        VV, EV, M> {
    	/** Retrieve graph traversal results. */
        void takeResponse(ITraversalResponse<R> response);
    	/** Get the starting vertex for graph traversal. */
        TraversalVertexQuery<K, VV> vertex();
    	/** Get the starting edges for graph traversal. */
        TraversalEdgeQuery<K, EV> edges();

        void broadcast(IGraphMessage<K, M> message);
    }

    interface TraversalVertexQuery<K, VV> extends VertexQuery<K, VV> {
    	/** Retrieve iterator of vertices in graph traversal. */
        Iterator<K> loadIdIterator();
    }

    interface TraversalEdgeQuery<K, EV> extends EdgeQuery<K, EV> {
    	/** Retrieve the corresponding graph traversal starting vertices by specifying the vertex ID. */
        TraversalEdgeQuery<K, EV> withId(K vertexId);
    }
}
```

## Example
```java
public class StaticGraphTraversalAllExample {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StaticGraphTraversalAllExample.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                PWindowSource<IVertex<Integer, Integer>> prVertices =
                        pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                                line -> {
                                    String[] fields = line.split(",");
                                    IVertex<Integer, Integer> vertex = new ValueVertex<>(Integer.valueOf(fields[0]),
                                            Integer.valueOf(fields[1]));
                                    return Collections.singletonList(vertex);
                                }), AllWindow.getInstance()).withParallelism(1);

                PWindowSource<IEdge<Integer, Integer>> prEdges =
                        pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                                line -> {
                                    String[] fields = line.split(",");
                                    IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                                            Integer.valueOf(fields[1]), 1);
                                    return Collections.singletonList(edge);
                                }), AllWindow.getInstance()).withParallelism(1);

                GraphViewDesc graphViewDesc = GraphViewBuilder
                        .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                        .withShardNum(1)
                        .withBackend(BackendType.Memory)
                        .build();

                PGraphWindow<Integer, Integer, Integer> graphWindow =
                        pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

                graphWindow.traversal(new VertexCentricTraversal<Integer, Integer, Integer, Integer, Integer>(3) {
                    @Override
                    public VertexCentricTraversalFunction<Integer, Integer, Integer, Integer,
                            Integer> getTraversalFunction() {
                        return new VertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer>() {

                            private VertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer, Integer> vertexCentricFuncContext;

                            @Override
                            public void open(
                                    VertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer, Integer> vertexCentricFuncContext) {
                                this.vertexCentricFuncContext = vertexCentricFuncContext;
                            }

                            @Override
                            public void init(ITraversalRequest<Integer> traversalRequest) {
                                this.vertexCentricFuncContext.takeResponse(
                                        new TraversalResponse(traversalRequest.getRequestId(), 1));
                            }
                            @Override
                            public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
                            }
                            @Override
                            public void finish() {
                            }
                            @Override
                            public void close() {
                            }
                        };
                    }

                    @Override
                    public VertexCentricCombineFunction<Integer> getCombineFunction() {
                        return null;
                    }
                }).start().sink(v -> {});
            }
        });

        IPipelineResult result = pipeline.execute();
        result.get();
    }
    public static class TraversalResponse implements ITraversalResponse<Integer> {
        private long responseId;
        private int response;
        public TraversalResponse(long responseId, int response) {
            this.responseId = responseId;
            this.response = response;
        }

        @Override
        public long getResponseId() {
            return responseId;
        }

        @Override
        public Integer getResponse() {
            return response;
        }

        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }

        @Override
        public String toString() {
            return "TraversalResponse{" + "responseId=" + responseId + ", response=" + response
                    + '}';
        }
    }

}
```