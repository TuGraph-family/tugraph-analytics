# Process API介绍
GeaFlow对外提供了一系列Process API，这些API和通用的流批类似，但不完全相同。我们在Source API中已有介绍，其构建出来的source是带有window的，因此GeaFlow所有的Process API也都带有window语义。

# 接口
| API | 接口说明 | 入参说明 |
| -------- | -------- | -------- |
| <R> PWindowStream<R> map(MapFunction<T, R> mapFunction)     | 通过实现mapFunction，可以将输入的T转换成R向下游输出。     |mapFunction：用户自定义转换逻辑，T表示输入类型，R表示输出类型|
| PWindowStream<T> filter(FilterFunction<T> filterFunction)     | 通过实现filterFunction，可以将不符合要求的T进行过滤。     |filterFunction：用户自定义过滤逻辑，T表示输入类型|
| <R> PWindowStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction)     | 通过实现flatMapFunction，可以将输入的一个T，生成n个R向下游输出。     |flatMapFunction：用户自定义实现逻辑，T表示输入类型，R表示输出类型。|
| PWindowStream<T> union(PStream<T> uStream)     | 用于实现将两个输入流进行合并。     |uStream：输入流，T表示输入流类型|
| PWindowBroadcastStream<T> broadcast()     | 将数据流进行广播。     |无|
| <KEY> PWindowKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction)     | 对输入的record，按照KEY进行shuffle输出。     |selectorFunction：用户自定义选取KEY的逻辑，T表示输入record类型，KEY表示定义的KEY类型。|
| PWindowStream<T> reduce(ReduceFunction<T> reduceFunction)     | 支持两种模式的reduce，对于批而言，其是基于当前一个window内的reduce聚合计算；而对于流而言，则是基于动态增量的全局reduce聚合计算（等同于flink的流式聚合计算）。GeaFlow默认是流聚合计算语义，如果需要批语义，用户可以通过参数开启。     |reduceFunction：用户自定义reduce聚合逻辑，T表示输入record类型。|
| <ACC, OUT> PWindowStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction)     | 支持两种模式的aggregate，对于批而言，其是基于当前一个window内的aggregate聚合计算；而对于流而言，则是基于动态增量的全局aggregate聚合计算（等同于flink的流式聚合计算）。GeaFlow默认是流聚合计算语义，如果需要批语义，用户可以通过参数开启。     |aggregateFunction：用户自定义聚合计算逻辑，T表示输入类型，ACC为聚合值类型，OUT表示输出类型。|
| PIncStreamView<T> materialize()     | 用于标识聚合计算是基于流还是批，默认无须调用该接口。     |无|



# 示例
```java

public class StreamUnionPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamUnionPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/union";
    public static final String REF_FILE_PATH = "data/reference/union";
    public static final String SPLIT = ",";

    public static void main(String[] args) {
        // 获取作业执行环境
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        // 执行作业提交
        IPipelineResult<?> result = submit(environment);
        result.get();
        // 关闭执行环境
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<String> streamSource =
                    pipelineTaskCxt.buildSource(new FileSource<String>("data/input"
                        + "/email_edge",
                        Collections::singletonList) {}, SizeTumblingWindow.of(5000));

                PWindowSource<String> streamSource2 =
                    pipelineTaskCxt.buildSource(new FileSource<String>("data/input"
                        + "/email_edge",
                        Collections::singletonList) {}, SizeTumblingWindow.of(5000));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    // 先将streamSource和streamSource2进行union合并。
                    .union(streamSource2)
                    // 对每条消息进行按照SPLIT分隔符进行解析，并将每条数据向下游分发。
                    .flatMap(new FlatMapFunction<String, Long>() {
                        @Override
                        public void flatMap(String value, Collector collector) {
                            String[] records = value.split(SPLIT);
                            for (String record : records) {
                                collector.partition(Long.valueOf(record));
                            }
                        }
                    })
                    // 构建tuple。
                    .map(p -> Tuple.of(p, p))
                    // 按照tuple作为key进行shuffle。
                    .keyBy(p -> p)
                    // 进行动态流式增量计算。
                    .aggregate(new AggFunc())
                    // 指定agg并发。
                    .withParallelism(conf.getInteger(AGG_PARALLELISM))
                    .map(v -> String.format("%s", v))
                    .sink(sink)
                    .withParallelism(conf.getInteger(SINK_PARALLELISM));
            }
        });

        return pipeline.execute();
    }

    public static class AggFunc implements
        AggregateFunction<Tuple<Long, Long>, Tuple<Long, Long>, Tuple<Long, Long>> {

        // 初始化和创建Accumulator。
        @Override
        public Tuple<Long, Long> createAccumulator() {
            return Tuple.of(0L, 0L);
        }

        // 将value add到accumulator中。
        @Override
        public void add(Tuple<Long, Long> value, Tuple<Long, Long> accumulator) {
            accumulator.setF0(value.f0);
            accumulator.setF1(value.f1 + accumulator.f1);
        }

        // 从accumulator中获取tuple2结果。
        @Override
        public Tuple<Long, Long> getResult(Tuple<Long, Long> accumulator) {
            return Tuple.of(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> merge(Tuple<Long, Long> a, Tuple<Long, Long> b) {
            return null;
        }
    }
}

```