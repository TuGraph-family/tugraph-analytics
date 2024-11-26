# Source API介绍
GeaFlow对外提供了Source API，在接口层面需要提供IWindow，用于构建相应的window source，用户可以通过实现SourceFunction来定义具体的源头读取逻辑。


# 接口

| API | 接口说明 | 入参说明 |
| -------- | -------- | -------- |
| PWindowSource<T> build(IPipelineContext pipelineContext, SourceFunction<T> sourceFunction, IWindow<T> window)     | 构建window source     |SourceFunction：定义source读取逻辑，GeaFlow内部已经实现了几种类型的source function，例如Collection、File等；<br>IWindow：当前支持SizeTumblingWindow和AllWindow两种类型，前者可以用于支持流式的读取窗口，后者是用于支持批量一次性读取完。   |



为了构建window source，用户一般可以直接通过IPipelineTaskContext提供的buildSource接口来实现。
```java
	// Interface.
	<T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window);

	// Example: 构建window size为2的collection source.
	List<String> words = Lists.newArrayList("hello", "world", "hello", "word");
	PWindowSource<String> source =
        pipelineTaskCxt.buildSource(new CollectionSource<String>(words) {},
            SizeTumblingWindow.of(2));
```

# 示例
```java
public class WindowStreamWordCount {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(WindowStreamWordCount.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration config = pipelineTaskCxt.getConfig();
                List<String> words = Lists.newArrayList("hello", "world", "hello", "word");
                // 通过内置的CollectionSource构建source，同时指定window类型为SizeTumblingWindow，window size为2。
                PWindowSource<String> source =
                    pipelineTaskCxt.buildSource(new CollectionSource<String>(words) {},
                        SizeTumblingWindow.of(2));
                source.sink(v -> LOGGER.info("result: {}", v));
            }
        });

        IPipelineResult result = pipeline.execute();
        // 等待执行完成
        result.get();
    }

}
```