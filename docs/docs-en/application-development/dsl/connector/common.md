# Introduction to Connector Basics
GeaFlow supports reading and writing data from various connectors. GeaFlow identifies them as external tables and stores the metadata in the Catalog.

# Syntax

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='file',
    geaflow.dsl.file.path = '/path/to/file',
    geaflow.dsl.window.size = 1000
)
```

Declare a table to use a Connector, and the specific use as a Source/Sink is determined by the read/write behavior. 

TEMPORARY is used to create a temporary table that is not stored in the Catalog. 

If IF NOT EXISTS is not specified, an error will be thrown if a table with the same name already exists. 

The WITH clause is used to specify the configuration information for the Connector. The type field is mandatory to specify the type of Connector to be used, for example, 'file' represents using a file. 

Additionally, we can add table parameters in the WITH clause. These parameters will override the external (SQL file, job parameters) configurations and have the highest priority.

# Common Options

| Key                                             | Required | Description                                     |
|-----------------------------------------------|----------|-------------------------------------------------|
| type                                          | true     | Specifies the type of Connector to be used                                |
| geaflow.dsl.window.size                       | false    | Important. -1 indicates reading all data as one window, which is batch processing. A positive value indicates several data as one window, which is stream processing. |
| geaflow.dsl.partitions.per.source.parallelism | false        | Groups several shards of the Source together to reduce the resource usage associated with concurrency.                |


# Example

```sql
CREATE TABLE console_sink (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='console'
);

-- Write one row to the log
INSERT INTO console_sink
SELECT 1, 'a', 2;
```