# Hbase Connector Introduction
The HBase Connector is contributed by the community and supports Sink yet.

# Syntax

```sql
CREATE TABLE hbase_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='hbase',
    geaflow.dsl.hbase.zookeeper.quorum = '127.0.0.1',
    geaflow.dsl.hbase.tablename = 'GeaFlowBase',
    geaflow.dsl.hbase.rowkey.column = 'id'
);
```
# Options

| Key | Required | Description |
| -------- | -------- | -------- |
| geaflow.dsl.hbase.zookeeper.quorum     | true     | HBase zookeeper quorum servers list.     |
| geaflow.dsl.hbase.namespace     | false     | HBase namespace.     |
| geaflow.dsl.hbase.tablename     | true     | HBase table name.     |
| geaflow.dsl.hbase.rowkey.column     | true     | HBase rowkey columns.     |
| geaflow.dsl.hbase.rowkey.separator     | false     | HBase rowkey join serapator.     |
| geaflow.dsl.hbase.familyname.mapping     | false     | HBase column family name mapping.     |
| geaflow.dsl.hbase.buffersize     | false     | HBase writer buffer size.     |

# Example

```sql
CREATE TABLE file_source (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='file',
    geaflow.dsl.file.path = '/path/to/file'
);

CREATE TABLE hbase_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='hbase',
    geaflow.dsl.hbase.zookeeper.quorum = '127.0.0.1',
    geaflow.dsl.hbase.tablename = 'GeaFlowBase',
    geaflow.dsl.hbase.rowkey.column = 'id'
);

INSERT INTO hbase_table
SELECT * FROM file_source;
```