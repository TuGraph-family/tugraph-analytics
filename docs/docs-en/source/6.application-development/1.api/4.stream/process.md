# Process Introduction
GeaFlow provides a series of Process APIs to the public, which are similar to general stream batch but not identical. As already introduced in the Source API, the source constructed from it has window semantics. Therefore, all GeaFlow Process APIs also have window semantics.

# Interface
| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| <R> PWindowStream<R> map(MapFunction<T, R> mapFunction)     | By implementing mapFunction, input T can be transformed into R and output to downstream     |mapFunction：Users define their own conversion logic, T represents input type, and R represents output type|
| PWindowStream<T> filter(FilterFunction<T> filterFunction)     | "By implementing filterFunction, T that does not meet the requirements can be filtered out     |filterFunction：Users define their own filter logic, T represents input type|
| <R> PWindowStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction)     | By implementing flatMapFunction, one T input can generate n R outputs and output to downstream     |flatMapFunction：Users implement their own logic, T represents input type, and R represents output type|
| PWindowStream<T> union(PStream<T> uStream)     | Used to implement merging two input streams     |uStream：Input stream, T represents the input stream type|
| PWindowBroadcastStream<T> broadcast()     | Broadcasting data streams     |No|
| <KEY> PWindowKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction)     | Shuffle the input records according to the KEY and output them     |selectorFunction：Users define their own logic for selecting KEY, T represents record input type, and KEY represents the defined KEY type|
| PWindowStream<T> reduce(ReduceFunction<T> reduceFunction)     | Support two modes of reduce. For batch, it is based on the reduction and aggregation calculation within the current window. For dynamic streams, it is based on the global reduction and aggregation calculation of dynamic increment (equivalent to Flink's stream aggregation calculation). By default, GeaFLOW adopts dynamic stream aggregation calculation semantics. If batch semantics are needed, users can enable them through parameters     |reduceFunction：Users define their own reduce aggregation logic. T indicates the input record type|
| <ACC, OUT> PWindowStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction)     | Support two modes of aggregate. For batch processing, it is based on the aggregate calculation within the current window; while for stream processing, it is based on the global aggregate calculation of dynamic increment (equivalent to Flink's stream aggregation calculation). By default, GeaFlow adopts stream aggregate calculation semantics. If batch semantics are needed, users can enable them through parameters     |aggregateFunction：Users define their own aggregation calculation logic, T represents input type, ACC represents aggregation value type, and OUT represents output type|
| PIncStreamView<T> materialize()     | Used to identify whether the aggregation calculation is based on streaming or batch processing. By default, there is no need to call this interface     |No|



# Example
```java
public class StreamUnionPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamUnionPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/union";
    public static final String REF_FILE_PATH = "data/reference/union";
    public static final String SPLIT = ",";

    public static void main(String[] args) {
        // Get execution environment.
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        // Perform job submission.
        IPipelineResult<?> result = submit(environment);
        result.get();
        // Wait for execution to complete.
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
                    // Union streamSource with streamSource2。
                    .union(streamSource2)
                    // Parse each message according to the SPLIT delimiter, and distribute each data downstream.
                    .flatMap(new FlatMapFunction<String, Long>() {
                        @Override
                        public void flatMap(String value, Collector collector) {
                            String[] records = value.split(SPLIT);
                            for (String record : records) {
                                collector.partition(Long.valueOf(record));
                            }
                        }
                    })
                    // Build tuple.
                    .map(p -> Tuple.of(p, p))
                    // Shuffle according to the tuple as the key.
                    .keyBy(p -> p)
                    // Perform dynamic stream incremental computation.
                    .aggregate(new AggFunc())
                    // Specify the parallelism for aggregation.
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

        // Initialize and create Accumulator.
        @Override
        public Tuple<Long, Long> createAccumulator() {
            return Tuple.of(0L, 0L);
        }

        // Add the value to the accumulator.
        @Override
        public void add(Tuple<Long, Long> value, Tuple<Long, Long> accumulator) {
            accumulator.setF0(value.f0);
            accumulator.setF1(value.f1 + accumulator.f1);
        }

        // Get Tuple2 result from accumulator.
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