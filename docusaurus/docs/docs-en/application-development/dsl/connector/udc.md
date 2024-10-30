# User Defined Connector
GeaFlow support user defined connector using the java SPI.
## Interface
### TableConnector
User should implement a **TableConnector**. We support **TableReadableConnector** for read and **TableWritableConnector** for write. If you implement both of them, the connector will support both read and write.

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
TableSource is the inferface for read data from the connector.

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
TableSink is the interface for write data to the connector.

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

# Example
Here is an example for console table connector.

## Implement TableConnector

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
After implement the **ConsoleTableConnector**, you should put the full class name to
the **resources/META-INF.services/com.antgroup.geaflow.dsl.connector.api.TableConnector**

## Usage

```java
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

