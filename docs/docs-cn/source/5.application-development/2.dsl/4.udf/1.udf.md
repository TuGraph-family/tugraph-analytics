# UDF介绍
UDF（User Defined Function）将标量值映射到标量值。
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

public abstract class UDF extends UserDefinedFunction {

}

```
## 示例

```java
public class ConcatWS extends UDF {

    public String eval(String... args) {
        String separator = args[0];
        String[] words = new String[args.length - 1];
        for (int index = 0; index < d.length; index++) {
            words[index] = args[index + 1];
        }
        return StringUtils.join(words, separator);
    }

}
```

```sql
Create Function my_cancat as 'com.antgroup.geaflow.dsl.udf.table.string.ConcatWS'; 

select my_cancat(',', '1', '2', '3');
```