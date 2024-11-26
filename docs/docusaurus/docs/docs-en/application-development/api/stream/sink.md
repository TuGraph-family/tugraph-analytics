# Sink Introduction
GeaFlow provides Sink API to the public, used to build Window Sink. Users can define specific output logic by implementing SinkFunction.

# Interface
| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| PStreamSink<T> sink(SinkFunction<T> sinkFunction)     | Output the result     |SinkFunction: Users can define their respective output semantics by implementing the SinkFunction interface. GeaFlow has integrated several sink functions internally, such as Console, File, etc.|
* Sink Usage
```java
	// Print the results directly to the console.
	source.sink(v -> {LOGGER.info("result: {}", v)});
```

# Example
```java
public class WindowStreamWordCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowStreamWordCount.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration config = pipelineTaskCxt.getConfig();
                List<String> words = Lists.newArrayList("hello", "world", "hello", "word");
                PWindowSource<String> source = pipelineTaskCxt.buildSource(new CollectionSource<String>(words) {
                }, SizeTumblingWindow.of(100));
                // Print the results directly to the console.
                source.sink(v -> {
                    LOGGER.info("result: {}", v);
                });
            }
        });

        IPipelineResult result = pipeline.execute();
        result.get();
    }
}
```
