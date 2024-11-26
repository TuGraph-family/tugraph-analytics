# Hive Connector Introduction
GeaFlow support read data from hive table through the hive metastore server. Currently we support Hive 2.3.x version.
# Syntax

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
# Options

| Key | Required | Description |
| -------- | -------- | -------- |
| geaflow.dsl.hive.database.name     | true     | The hive database name.     |
| geaflow.dsl.hive.table.name     | true     | The hive table name.     |
| geaflow.dsl.hive.metastore.uris     | true     | The hive metastore uris     |
| geaflow.dsl.hive.splits.per.partition     | false     | The number of splits for each hive partition.Default value is 1.     |

# Example

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