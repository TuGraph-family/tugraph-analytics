# UDGA介绍
UDGA（User Define Graph Algorithm）定义了一个图算法，例如SSSP（单源最短路径）和PageRank算法。
## 接口

```java
/**
 * Interface for the User Defined Graph Algorithm.
 * @param <K> The id type for vertex.
 * @param <M> The message type for message send between vertices.
 */
public interface AlgorithmUserFunction<K, M> extends Serializable {

    /**
     * Init method for the function
     * @param context The runtime context.
     * @param params  The parameters for the function.
     */
    void init(AlgorithmRuntimeContext<K, M> context, Object[] params);

    /**
     * Processing method for each vertex and the messages it received.
     */
    void process(RowVertex vertex, Iterator<M> messages);

    /**
     * Returns the output type for the function.
     */
    StructType getOutputType();
}

```

## 示例

```java
public class PageRank implements AlgorithmUserFunction {

    private AlgorithmRuntimeContext context;
    private double alpha = 0.85;
    private double convergence = 0.01;
    private int iteration = 20;

    @Override
    public void init(AlgorithmRuntimeContext context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 3) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([alpha, [convergence, [max_iteration]]])");
        }
        if (parameters.length > 0) {
            alpha = Double.parseDouble(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            convergence = Double.parseDouble(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            iteration = Integer.parseInt(String.valueOf(parameters[2]));
        }
    }

    @Override
    public void process(RowVertex vertex, Iterator messages) {
        List<RowEdge> outEdges = new ArrayList<>(context.loadEdges(EdgeDirection.OUT));
        outEdges.addAll(context.loadEdges(EdgeDirection.BOTH));
        if (context.getCurrentIterationId() == 1L) {
            double initValue = 1.0;
            sendMessageToNeighbors(outEdges, 1.0 / outEdges.size());
            sendMessageToNeighbors(outEdges, -1.0);
            context.updateVertexValue(ObjectRow.create(initValue));
        } else if (context.getCurrentIterationId() <= iteration) {
            double sum = 0.0;
            while (messages.hasNext()) {
                double input = (double) messages.next();
                input = input > 0 ? input : 0.0;
                sum += input;
            }
            double pr = (1 - alpha) + (sum * alpha);
            double currentPr = (double) vertex.getValue().getField(0, 
								DoubleType.INSTANCE);
            if (Math.abs(currentPr - pr) > convergence) {
                sendMessageToNeighbors(outEdges, pr / outEdges.size());
            }
            sendMessageToNeighbors(outEdges, -1.0);
            context.updateVertexValue(ObjectRow.create(pr));
        } else {
            double currentPr = (double) vertex.getValue().getField(0, 
								DoubleType.INSTANCE);
            context.take(ObjectRow.create(vertex.getId(), currentPr));
            return;
        }
    }

    @Override
    public StructType getOutputType() {
        return new StructType(
            new TableField("id", LongType.INSTANCE, false),
            new TableField("pr", DoubleType.INSTANCE, false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> outEdges, Object message) {
        for (RowEdge rowEdge : outEdges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
```

```sql
CREATE Function my_page_rank AS 'com.antgroup.geaflow.dsl.udf.graph.PageRank';

INSERT INTO tbl_result
CALL my_page_rank(1) YIELD (vid, prValue)
RETURN vid, ROUND(prValue, 2)
;
```