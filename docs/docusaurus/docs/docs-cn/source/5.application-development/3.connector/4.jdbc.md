# JDBC Connector介绍
JDBC Connector由社区贡献，支持读和写。
## 语法

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
## 参数

| 参数名 | 是否必须 | 描述                                                |
| -------- |------|---------------------------------------------------|
| geaflow.dsl.jdbc.driver     | 是    | The JDBC driver.                                  |
| geaflow.dsl.jdbc.url     | 是    | The database URL.                                 |
| geaflow.dsl.jdbc.username     | 是    | The database username.                            |
| geaflow.dsl.jdbc.password     | 是    | The database password.                            |
| geaflow.dsl.jdbc.table.name     | 是    | The table name.                                   |
| geaflow.dsl.jdbc.partition.num     | 否    | The JDBC partition number, default 1.             |
| geaflow.dsl.jdbc.partition.column     | 否    | The JDBC partition column. Default value is 'id'. |
| geaflow.dsl.jdbc.partition.lowerbound     | 否    | The lowerbound of JDBC partition, just used to decide the partition stride, not for filtering the rows in table.                                 |
| geaflow.dsl.jdbc.partition.upperbound     | 否    | The upperbound of JDBC partition, just used to decide the partition stride, not for filtering the rows in table.                            |


## 示例

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