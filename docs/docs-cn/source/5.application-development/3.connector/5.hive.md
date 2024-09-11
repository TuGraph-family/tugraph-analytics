# Hive Connector介绍
GeaFlow 支持通过 Hive metastore 服务器读取 Hive 表中的数据。目前，我们支持 Hive 2.3.x系列版本。
## 语法

```sql
CREATE TABLE hive_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='hive',
    geaflow.dsl.hive.database.name = 'default',
	geaflow.dsl.hive.table.name = 'user',
	geaflow.dsl.hive.metastore.uris = 'thrift://localhost:9083'
)
```
## 参数

| 参数名 | 是否必须 | 描述 |
| -------- | ---- | -------- |
| geaflow.dsl.hive.database.name     | 是 | Hive数据库名字     |
| geaflow.dsl.hive.table.name     | 是 | Hive表名     |
| geaflow.dsl.hive.metastore.uris     | 是 | 连接Hive元数据metastore的uri列表     |
| geaflow.dsl.hive.splits.per.partition     | 否 | 每个Hive分片的逻辑分片数量，默认为1     |

## 示例

```sql
CREATE TABLE hive_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='hive',
    geaflow.dsl.hive.database.name = 'default',
	geaflow.dsl.hive.table.name = 'user',
	geaflow.dsl.hive.metastore.uris = 'thrift://localhost:9083'
);

CREATE TABLE console (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='console'
);

INSERT INTO console
SELECT * FROM hive_table;
```