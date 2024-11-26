# Graph Compute Introduction
GeaFlow provides interfaces for implementing graph computing algorithms, and static or dynamic graph computing can be performed by implementing the corresponding interfaces. Users can define specific computing logic and maximum iteration times in the compute algorithm.

# Dynamic Graph
## Interface
| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| void init(IncGraphComputeContext<K, VV, EV, M> incGraphContext) | Graph computing initialization interface | incGraphContext: Context for incremental dynamic graph computing, where K represents the type of vertex ID, VV represents the type of vertex value, EV represents the type of edge value, and M represents the type of message to be sent |
| void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph) | First round iteration for incremental graph processing logic implementation | vertexId: The ID of the current vertex point, where K represents the type of vertex ID <br>temporaryGraph: Temporary incremental graph, where K represents the type of vertex ID, VV represents the type of vertex value, and EV represents the type of edge value|
| void compute(K vertexId, Iterator messageIterator) | Iterative computing interface | vertexId: The ID of the current computation point, where K represents the type of vertex ID |
| void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph) | Iterative computing complete interface | vertexId: The ID of the current computation vertex, where K represents the type of vertex ID <br>mutableGraph: Mutable graph, where K represents the type of vertex ID, VV represents the type of vertex value, and EV represents the type of edge value |



* Detailed interface
```java

public interface IncVertexCentricFunction<K, VV, EV, M> extends Function {

   void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph);

   void compute(K vertexId, Iterator<M> messageIterator);

   void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph);

   interface IncGraphContext<K, VV, EV, M> {
       /** Get job id. */
       long getJobId();

        /** Get the current iterartion id. */
        long getIterationId();
        
        /** Get the runtime context. */
        RuntimeContext getRuntimeContext();

        /** Get the mutable graph. */
        MutableGraph<K, VV, EV> getMutableGraph();

    	/** Get the temporary graph. */
        TemporaryGraph<K, VV, EV> getTemporaryGraph();

        /** Get the historical graph. */
        HistoricalGraph<K, VV, EV> getHistoricalGraph();

        /** Send message to specified vertex. */
        void sendMessage(K vertexId, M message);

        /** Send message to neighbors of current vertex. */
        void sendMessageToNeighbors(M message);

   }

   interface TemporaryGraph<K, VV, EV> {
   /** Get vertex from temporary graph. */
   IVertex<K, VV> getVertex();

        /** Get the edges from incremental graph. */
        List<IEdge<K, EV>> getEdges();

        /** Update vertex value. */
        void updateVertexValue(VV value);

   }

   interface HistoricalGraph<K, VV, EV> {
   /** Get the latest version id of graph state. */
   Long getLatestVersionId();

        /** Get all versions of graph state. */
        List<Long> getAllVersionIds();

        /** Get all vertices. */
        Map<Long, IVertex<K, VV>> getAllVertex();

        /** Get the all vertices of specified version. */
        Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions);

        /** Get vertices of the graph data of a specified version that meet the filtering condition. */
        Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions, IVertexFilter<K, VV> vertexFilter);

        /** Get snapshot of the graph data of a specified version. */
        GraphSnapShot<K, VV, EV> getSnapShot(long version);

   }

   interface GraphSnapShot<K, VV, EV> {
   /** Get the current version id. */
   long getVersion();
   /** Get vertex. */
   VertexQuery<K, VV> vertex();
   /** Get edges. */
   EdgeQuery<K, EV> edges();

   }

   interface MutableGraph<K, VV, EV> {
   /** Add a vertex to the graph and specify its version ID. */
   void addVertex(long version, IVertex<K, VV> vertex);
   /** Add a edge to the graph and specify its version ID. */
   void addEdge(long version, IEdge<K, EV> edge);

   }


}

public interface IncVertexCentricComputeFunction<K, VV, EV, M> extends
        IncVertexCentricFunction<K, VV, EV, M> {

    void init(IncGraphComputeContext<K, VV, EV, M> incGraphContext);

    interface IncGraphComputeContext<K, VV, EV, M> extends IncGraphContext<K, VV, EV, M> {
        void collect(IVertex<K, VV> vertex);
    }

}
```

## Example

```java
public class IncrGraphCompute {

   private static final Logger LOGGER = LoggerFactory.getLogger(IncrGraphCompute.class);

   public static void main(String[] args) {
      Environment environment = EnvironmentFactory.onLocalEnvironment();
      IPipelineResult result = submit(environment);
      result.get();
      environment.shutdown();
   }

   public static IPipelineResult<?> submit(Environment environment) {
      final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
      final String graphName = "graph_view_name";
      GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
              .withShardNum(4)
              .withBackend(BackendType.RocksDB)
              .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class, Integer.class, ValueEdge.class, IntegerType.class))
              .build();
      pipeline.withView(graphName, graphViewDesc);
      pipeline.submit(new PipelineTask() {
         @Override
         public void execute(IPipelineTaskContext pipelineTaskCxt) {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Integer, Integer>> vertices =
                    // Extract vertex from edge file.
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

            PGraphView<Integer, Integer, Integer> fundGraphView = pipelineTaskCxt.getGraphView(graphName);

            PIncGraphView<Integer, Integer, Integer> incGraphView = fundGraphView.appendGraph(vertices, edges);
            incGraphView.incrementalCompute(new IncGraphAlgorithms(3))
                    .getVertices()
                    .map(v -> String.format("%s,%s", v.getId(), v.getValue()))
                    .sink(v -> {
                       LOGGER.info("result: {}", v);
                    });
         }
      });
      return pipeline.execute();
   }


   public static class IncGraphAlgorithms extends IncVertexCentricCompute<Integer, Integer, Integer, Integer> {
      public IncGraphAlgorithms(long iterations) {
         super(iterations);
      }

      @Override
      public IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> getIncComputeFunction() {
         return new IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer>() {
            private IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext;

            @Override
            public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext) {
               this.graphContext = graphContext;
            }
            @Override
            public void evolve(Integer vertexId,
                               TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
               IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
               if (vertex != null) {
                  if (temporaryGraph.getEdges() != null) {
                     for (IEdge<Integer, Integer> edge : temporaryGraph.getEdges()) {
                        graphContext.sendMessage(edge.getTargetId(), vertexId);
                     }
                  }
               }
            }

            @Override
            public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
               int max = 0;
               while (messageIterator.hasNext()) {
                  int value = messageIterator.next();
                  max = Math.max(max, value);
               }
               graphContext.getTemporaryGraph().updateVertexValue(max);
            }

            @Override
            public void finish(Integer vertexId, MutableGraph<Integer, Integer, Integer> mutableGraph) {
               IVertex<Integer, Integer> vertex = graphContext.getTemporaryGraph().getVertex();
               graphContext.collect(vertex);
            }
         };
      }
      @Override
      public VertexCentricCombineFunction<Integer> getCombineFunction() {
         return null;
      }

   }
}
```

# Static Graph
## Interface
| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| void init(VertexCentricComputeFuncContext<K, VV, EV, M> vertexCentricFuncContext) | Iterative computing initialization interface | vertexCentricFuncContext: Context for static graph computing, where K represents the type of vertex ID, VV represents the type of vertex value, EV represents the type of edge value, and M represents the type of message to be sent |
| void compute(K vertexId, Iterator messageIterator) | Iterative computing interface | vertexId: The ID of the current computation vertex, where K represents the type of vertex ID <br>messageIterator: All messages sent to the current vertex during iteration, where M represents the type of message defined during iterative computing|
| void finish() | Iterative computing complete interface | no |


* Detailed interface

```java
public interface VertexCentricComputeFunction<K, VV, EV, M> extends VertexCentricFunction<K, VV,
EV, M> {

    void init(VertexCentricComputeFuncContext<K, VV, EV, M> vertexCentricFuncContext);

    void compute(K vertex, Iterator<M> messageIterator);

    void finish();

    interface VertexCentricComputeFuncContext<K, VV, EV, M> extends VertexCentricFuncContext<K, VV,
        EV, M> {
    	/** Set new vertex value. */
        void setNewVertexValue(VV value);

    }

}
```

## Example
```java
public class StaticsGraphCompute {
    
    public static void main(String[] args) {
      	Environment environment = EnvironmentFactory.onLocalEnvironment();
        IPipelineResult result = submit(environment);
        result.get();
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            PWindowSource<IVertex<Integer, Integer>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Integer> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                    .withParallelism(2);

            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(new FileSource<>(
                "data/input/email_edge", line -> {
                String[] fields = line.split(",");
                IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                return Collections.singletonList(edge);
            }), AllWindow.getInstance()).withParallelism(2);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(2)
                .withBackend(BackendType.Memory)
                .build();
            
            PGraphWindow<Integer, Integer, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);
            graphWindow.compute(new SSSPAlgorithm(1, 10))
                .compute(2)
                .getVertices()
                .sink(v -> {});
        });
        return pipeline.execute();
    }
    
    public static class SSSPAlgorithm extends VertexCentricCompute<Integer, Integer, Integer, Integer> {

        private final int srcId;
        public SSSPAlgorithm(int srcId, long iterations) {
            super(iterations);
            this.srcId = srcId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Integer, Integer, Integer> getComputeFunction() {
            return new VertexCentricComputeFunction<Integer, Integer, Integer, Integer>() {
                
                private VertexCentricComputeFuncContext<Integer, Integer, Integer, Integer> context;
                @Override
                public void init(VertexCentricComputeFuncContext<Integer, Integer, Integer, Integer> vertexCentricFuncContext) {
                    this.context = vertexCentricFuncContext;
                }

                @Override
                public void compute(Integer vertex, Iterator<Integer> messageIterator) {
                    int minDistance = vertex == srcId ? 0 : Integer.MAX_VALUE;
                    if (messageIterator != null) {
                        while (messageIterator.hasNext()) {
                            Integer value = messageIterator.next();
                            minDistance = Math.min(minDistance, value);
                        }
                    }
                    IVertex<Integer, Integer> iVertex = this.context.vertex().get();
                    if (minDistance < iVertex.getValue()) {
                        this.context.setNewVertexValue(minDistance);
                        for (IEdge<Integer, Integer> edge : this.context.edges().getOutEdges()) {
                            this.context.sendMessage(edge.getTargetId(), minDistance + edge.getValue());
                        }
                    }
                }
                @Override
                public void finish() {
                    
                }
            };
        }
        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }
    }
}
```