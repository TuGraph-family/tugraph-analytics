# 自定义Connector

GeaFlow 支持使用Java SPI方式定义用户自定义Connector。
## 接口
### 表Connector
用户应该实现一个 TableConnector 接口。我们支持使用 TableReadableConnector 用于读取数据，使用 TableWritableConnector 用于写入数据。如果两个接口都实现了，连接器将同时支持读和写操作。

```java
/**
 * The interface for table connector.
 */
public interface TableConnector {

    /**
     * Return table connector type.
     */
    String getType();
}

/**
 * A readable table connector.
 */
public interface TableReadableConnector extends TableConnector {

    TableSource createSource(Configuration conf);
}

/**
 * A writable table connector.
 */
public interface TableWritableConnector extends TableConnector {

    /**
     * Create the {@link TableSink} for the table connector.
     */
    TableSink createSink(Configuration conf);
}
```

## TableSource
TableSource 接口用于从连接器中读取数据。

```java
/**
 * Interface for table source.
 */
public interface TableSource extends Serializable {

    /**
     * The init method for compile time.
     */
    void init(Configuration tableConf, TableSchema tableSchema);

    /**
     * The init method for runtime.
     */
    void open(RuntimeContext context);

    /**
     * List all the partitions for the source.
     */
    List<Partition> listPartitions();

    /**
     * Returns the {@link TableDeserializer} for the source to convert data read from
     * the source to {@link Row}.
     */
    <IN> TableDeserializer<IN> getDeserializer(Configuration conf);

    /**
     * Fetch data for the partition from start offset. if the windowSize is -1, it represents an
     * all-window which will read all the data from the source, else return widow size for data.
     */
    <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, long windowSize) throws IOException;

    /**
     * The close callback for the job finish the execution.
     */
    void close();
}

```

## TableSink
TableSink 接口用于将数据写入连接器。

```java
/**
 * Interface for table sink.
 */
public interface TableSink extends Serializable {

    /**
     * The init method for compile time.
     */
    void init(Configuration tableConf, StructType schema);

    /**
     * The init method for runtime.
     */
    void open(RuntimeContext context);

    /**
     * The write method for writing row to the table.
     */
    void write(Row row) throws IOException;

    /**
     * The finish callback for each window finished.
     */
    void finish() throws IOException;

    /**
     * The close callback for the job finish the execution.
     */
    void close();
}
```

## 示例
下面是一个用于控制台的Table Connector的示例。
### 实现

```java
public class ConsoleTableConnector implements TableWritableConnector {

    @Override
    public String getType() {
        return "CONSOLE";
    }

    @Override
    public TableSink createSink(Configuration conf) {
        return new ConsoleTableSink();
    }
}

public class ConsoleTableSink implements TableSink {

    private static final Logger LOGGER = 
			LoggerFactory.getLogger(ConsoleTableSink.class);

    private boolean skip;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        skip = tableConf.getBoolean(ConsoleConfigKeys.GEAFLOW_DSL_CONSOLE_SKIP);
    }

    @Override
    public void open(RuntimeContext context) {

    }

    @Override
    public void write(Row row) {
        if (!skip) {
            LOGGER.info(row.toString());
        }
    }

    @Override
    public void finish() {

    }

    @Override
    public void close() {

    }
}
```
在实现了 ConsoleTableConnector 后，您需要将完整的类名添加到 resources/META-INF.services/com.antgroup.geaflow.dsl.connector.api.
TableConnector 文件中。该文件应列出所有实现了 TableConnector 接口的连接器类的全名，以便 GeaFlow 在启动时能够扫描到这些类，并将它们注册为可用的Connector。

### 用法

```sql
CREATE TABLE file_source (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
    type='file',
    geaflow.dsl.file.path = '/path/to/file'
);

CREATE TABLE console_sink (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
    type='console'
);

INSERT INTO console_sink
SELECT * FROM file_source;
```
