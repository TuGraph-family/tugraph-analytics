# UDTF介绍
UDTF（User Defined Table Function）将输入扩展为多行。
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

public abstract class UDTF extends UserDefinedFunction {

    protected List<Object[]> collector;

    public UDTF() {
        this.collector = Lists.newArrayList();
    }
	
	/**
     * Collect the result.
     */
    protected void collect(Object[] output) {
        
    }
	
	/**
     * Returns type output types for the function.
     * @param paramTypes The parameter types of the function.
     * @param outFieldNames The output fields of the function in the sql.
     */
    public abstract List<Class<?>> getReturnType(List<Class<?>> paramTypes, 
												 List<String> outFieldNames);
}

```
每个UDTF都应该有一个或多个**eval**方法。

## 示例

```java
public class Split extends UDTF {

    private String splitChar = ",";

    public void eval(String text) {
        evalInternal(text);
    }

    public void eval(String text, String separator) {
        evalInternal(text, separator);
    }

    private void evalInternal(String... args) {
        if (args != null && (args.length == 1 || args.length == 2)) {
            if (args.length == 2 && StringUtils.isNotEmpty(args[1])) {
                splitChar = args[1];
            }
            String[] lines = StringUtils.split(args[0], splitChar);
            for (String line : lines) {
                collect(new Object[]{line});
            }
        }
    }

    @Override
    public List<Class<?>> getReturnType(List<Class<?>> paramTypes, 
										List<String> outputFields) {
        return Collections.singletonList(String.class);
    }
}
```

```sql
CREATE Function my_split AS 'com.antgroup.geaflow.dsl.udf.Split';

SELECT t.id, u.name FROM users u, LATERAL table(my_split(u.ids)) as t(id);
```