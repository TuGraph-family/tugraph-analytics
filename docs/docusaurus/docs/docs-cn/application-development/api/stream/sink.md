# Sink API介绍
GeaFlow对外提供了Sink API，用于构建Window Sink，用户可以通过实现SinkFunction来定义具体的输出逻辑。

# 接口
| API | 接口说明 | 入参说明 |
| -------- | -------- | -------- |
| PStreamSink<T> sink(SinkFunction<T> sinkFunction)     | 将结果进行输出。     |sinkFunction：用户可以通过实现SinkFunction接口，用于定义其相应的输出语义。GeaFlow内部集成了几种sink function，例如：Console、File等。|
* Sink用法
```java
	// 将结果直接打印到console。
	source.sink(v -> {LOGGER.info("result: {}", v)});
```

# 示例
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
                // 将结果直接打印在console
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
