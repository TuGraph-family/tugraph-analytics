# Compute API介绍
GeaFlow对外提供了实现图计算算法的接口，通过实现相应接口可进行静态图计算或动态图计算，用户可在compute算法中定义具体的计算逻辑及迭代最大次数。

# 动态图
## 接口
| API | 接口说明 | 入参说明 |
| --- | --- | --- |
| void init(IncGraphComputeContext<K, VV, EV, M> incGraphContext) | 图计算初始化接口 | incGraphContext： 增量动态图计算的上下文，K表示vertex id的类型，VV表示vertex value类型，EV表示edge value类型，M表示发送消息的类型。 |
| void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph) | 首轮迭代对增量图实现处理逻辑 | vertexId：当前计算点的id，其中K表示vertex id的类型。<br>temporaryGraph：临时增量图，其中K表示vertexId的类型，VV表示vertex value类型，EV表示edge value类型。 |
| void compute(K vertexId, Iterator messageIterator) | 迭代计算接口 | vertexId：当前计算点的id，其中K表示vertex id的类型。 |
| void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph) | 迭代计算完成接口 | vertexId：当前计算点的id，其中K表示vertex id的类型。<br>mutableGraph：可变图，其中K表示vertexId的类型，VV表示vertex value类型，EV表示edge value类型 |


- 详细接口

```java
public interface IncVertexCentricFunction<K, VV, EV, M> extends Function {

   void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph);

   void compute(K vertexId, Iterator<M> messageIterator);

   void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph);

   interface IncGraphContext<K, VV, EV, M> {
       /** 获取job id */
       long getJobId();

        /** 获取当前迭代 id */
        long getIterationId();
        
        /** 获取运行时上下文 */
        RuntimeContext getRuntimeContext();

        /** 获取可变图 */
        MutableGraph<K, VV, EV> getMutableGraph();

    	/** 获取增量图 */
        TemporaryGraph<K, VV, EV> getTemporaryGraph();

        /** 获取图存储上的历史图 */
        HistoricalGraph<K, VV, EV> getHistoricalGraph();

        /** 给指定vertex发送消息 */
        void sendMessage(K vertexId, M message);

        /** 给当前vertex邻居节点发送消息 */
        void sendMessageToNeighbors(M message);

   }

   interface TemporaryGraph<K, VV, EV> {
   /** 从增量图中获取vertex */
   IVertex<K, VV> getVertex();

        /** 从增量图中获取edges */
        List<IEdge<K, EV>> getEdges();

        /** 更新vertex value */
        void updateVertexValue(VV value);

   }

   interface HistoricalGraph<K, VV, EV> {
   /** 获取图数据最新版本id */
   Long getLatestVersionId();

        /** 获取图数据所有版本 */
        List<Long> getAllVersionIds();

        /** 获取图数据所有vertex */
        Map<Long, IVertex<K, VV>> getAllVertex();

        /** 获取图数据指定版本的vertex */
        Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions);

        /** 获取图数据指定版本并满足过滤条件的vertex */
        Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions, IVertexFilter<K, VV> vertexFilter);

        /** 获取图数据指定版本的快照 */
        GraphSnapShot<K, VV, EV> getSnapShot(long version);

   }

   interface GraphSnapShot<K, VV, EV> {
   /** 获取当前版本id */
   long getVersion();
   /** 获取vertex */
   VertexQuery<K, VV> vertex();
   /** 获取edges */
   EdgeQuery<K, EV> edges();

   }

   interface MutableGraph<K, VV, EV> {
   /** 向图中添加vertex，并指定其版本id */
   void addVertex(long version, IVertex<K, VV> vertex);
   /** 向图中添加edge，并指定其版本id */
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

## 示例

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
                    // extract vertex from edge file
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

# 静态图

## 接口
| API | 接口说明 | 入参说明 |
| --- | --- | --- |
| void init(VertexCentricComputeFuncContext<K, VV, EV, M> vertexCentricFuncContext) | 迭代计算初始化接口 | vertexCentricFuncContext：静态图计算的上下文，K表示vertex id的类型，VV表示vertex value类型，EV表示edge value类型，M表示发送消息的类型。 |
| void compute(K vertexId, Iterator messageIterator) | 迭代计算接口 | vertexId：当前计算点的id，其中K表示vertex id的类型。<br>messageIterator：迭代过程中所有发送给当前vertex的消息，其中M表示迭代计算过程中定义的发送消息类型。 |
| void finish() | 迭代计算完成接口 | 无 |


- 详细接口

```java
public interface VertexCentricComputeFunction<K, VV, EV, M> extends VertexCentricFunction<K, VV,
EV, M> {

    void init(VertexCentricComputeFuncContext<K, VV, EV, M> vertexCentricFuncContext);

    void compute(K vertex, Iterator<M> messageIterator);

    void finish();

    interface VertexCentricComputeFuncContext<K, VV, EV, M> extends VertexCentricFuncContext<K, VV,
        EV, M> {
    	/** 设置vertex value */
        void setNewVertexValue(VV value);

    }

}
```

## 示例

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
