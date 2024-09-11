# Hbase Connector介绍
Hbase Connector由社区贡献，目前仅支持Sink。

## 语法示例

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
## 参数

| 参数名 | 是否必须 | 描述 |
| -------- | -------- | -------- |
| geaflow.dsl.hbase.zookeeper.quorum     | 是     | HBase zookeeper quorum servers list.     |
| geaflow.dsl.hbase.namespace     | 否     | HBase namespace.     |
| geaflow.dsl.hbase.tablename     | 是     | HBase table name.     |
| geaflow.dsl.hbase.rowkey.column     | 是     | HBase rowkey columns.     |
| geaflow.dsl.hbase.rowkey.separator     | 否     | HBase rowkey join serapator.     |
| geaflow.dsl.hbase.familyname.mapping     | 否     | HBase column family name mapping.     |
| geaflow.dsl.hbase.buffersize     | 否     | HBase writer buffer size.     |

## 示例

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