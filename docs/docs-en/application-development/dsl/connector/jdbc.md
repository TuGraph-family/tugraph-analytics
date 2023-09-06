# JDBC Connector Introduction
The JDBC Connector is contributed by the community and supports both reading and writing operations.
# Syntax

```sql
CREATE TABLE jdbc_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='jdbc',
    geaflow.dsl.jdbc.driver = 'org.h2.Driver',
    geaflow.dsl.jdbc.url = 'jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1',
    geaflow.dsl.jdbc.username = 'h2_user',
    geaflow.dsl.jdbc.password = 'h2_pwd',
    geaflow.dsl.jdbc.table.name = 'source_table'
);
```
# Options

| Key | Required | Description |
| -------- |------|---------------------------------------------------|
| geaflow.dsl.jdbc.driver     | true    | The JDBC driver.                                  |
| geaflow.dsl.jdbc.url     | true    | The database URL.                                 |
| geaflow.dsl.jdbc.username     | true    | The database username.                            |
| geaflow.dsl.jdbc.password     | true    | The database password.                            |
| geaflow.dsl.jdbc.table.name     | true    | The table name.                                   |
| geaflow.dsl.jdbc.partition.num     | false    | The JDBC partition number, default 1.             |
| geaflow.dsl.jdbc.partition.column     | false    | The JDBC partition column. Default value is 'id'. |
| geaflow.dsl.jdbc.partition.lowerbound     | false    | The lowerbound of JDBC partition, just used to decide the partition stride, not for filtering the rows in table.                                 |
| geaflow.dsl.jdbc.partition.upperbound     | false    | The upperbound of JDBC partition, just used to decide the partition stride, not for filtering the rows in table.                            |


# Example

```sql
set geaflow.dsl.jdbc.driver = 'org.h2.Driver';
set geaflow.dsl.jdbc.url = 'jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1';
set geaflow.dsl.jdbc.username = 'h2_user';
set geaflow.dsl.jdbc.password = 'h2_pwd'; 

CREATE TABLE jdbc_source_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='jdbc',
    geaflow.dsl.jdbc.table.name = 'source_table',
);

CREATE TABLE jdbc_sink_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='jdbc',
    geaflow.dsl.jdbc.table.name = 'sink_table'
);

INSERT INTO jdbc_sink_table
SELECT * FROM jdbc_source_table;
```