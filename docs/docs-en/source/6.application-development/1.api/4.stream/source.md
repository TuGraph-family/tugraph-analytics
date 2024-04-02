# Source Introduction
GeaFlow provides Source API to the public, and IWindow needs to be provided at the interface level to build the corresponding window source. Users can define the specific source reading logic by implementing SourceFunction.


# Interface

| API | Interface Description | Input Parameter Description |
| -------- | -------- | -------- |
| PWindowSource<T> build(IPipelineContext pipelineContext, SourceFunction<T> sourceFunction, IWindow<T> window)     | Build window source     |SourceFunction: Define source reading logic. GeaFlow has already implemented several types of source function internally, such as Collection, File, etc. <br> IWindow: There are currently two types supported, SizeTumblingWindow and AllWindow. The former can be used to support streaming reading windows, and the latter is used to support batch one-time reading.|


To build a window source, users can generally use the buildSource interface provided by IPipelineTaskContext directly.
```java
	// Interface.
	<T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window);

	// Example: Build a collection source with a window size of 2.
	List<String> words = Lists.newArrayList("hello", "world", "hello", "word");
	PWindowSource<String> source =
        pipelineTaskCxt.buildSource(new CollectionSource<String>(words) {},
            SizeTumblingWindow.of(2));
```

# Example
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
                // Build source using the built-in CollectionSource, while specifying the window type as SizeTumblingWindow and window size as 2.
                PWindowSource<String> source =
                    pipelineTaskCxt.buildSource(new CollectionSource<String>(words) {},
                        SizeTumblingWindow.of(2));
                source.sink(v -> LOGGER.info("result: {}", v));
            }
        });

        IPipelineResult result = pipeline.execute();
        // Wait for execution to complete.
        result.get();
    }

}
```