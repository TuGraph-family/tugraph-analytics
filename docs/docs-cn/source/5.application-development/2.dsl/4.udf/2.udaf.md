# UDAF介绍
UDAF（User Define Aggregate Function）将多行数据聚合为单个值。
## 接口

```java
public abstract class UserDefinedFunction implements Serializable {

   /**
     * Init method for the user defined function.
     */
    public void open(FunctionContext context) {
    }

    /**
     * Close method for the user defined function.
     */
    public void close() {
    }
}

public abstract class UDAF<InputT, AccumT, OutputT> extends UserDefinedFunction {

    /**
     * Create aggregate accumulator for aggregate function to store the aggregate value.
     */
    public abstract AccumT createAccumulator();

    /**
     * Accumulate the input to the accumulator.
     */
    public abstract void accumulate(AccumT accumulator, InputT input);

    /**
     * Merge the accumulator iterator to the accumulator.
     * @param accumulator The accumulator to merged to.
     * @param its The accumulator iterators to merge from.
     */
    public abstract void merge(AccumT accumulator, Iterable<AccumT> its);

    /**
     * Reset the accumulator to init value.
     */
    public abstract void resetAccumulator(AccumT accumulator);

    /**
     * Get aggregate function result from the accumulator.
     */
    public abstract OutputT getValue(AccumT accumulator);

}

```

## 示例

```java
public class AvgDouble extends UDAF<Double, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public void accumulate(Accumulator accumulator, Double input) {
        if (null != input) {
            accumulator.sum += input;
            accumulator.count ++;
        }
    }

    @Override
    public void merge(Accumulator merged, Iterable<Accumulator> accumulators) {
        for (Accumulator accumulator : accumulators) {
            merged.sum += accumulator.sum;
            merged.count += accumulator.count;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.sum = 0.0;
        accumulator.count = 0L;
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        return accumulator.count == 0 ? null : 
			(accumulator.sum / (double) accumulator.count);
    }

    public static class Accumulator implements Serializable {
        public double sum = 0.0;
        public long count = 0;
    }
}
```

```sql
CREATE Function my_avg AS 'com.antgroup.geaflow.dsl.udf.table.agg.AvgDouble';

SELECT my_avg(age) from user;
```