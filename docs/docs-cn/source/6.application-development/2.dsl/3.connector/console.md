# Console Connector介绍

## 语法

```sql
CREATE TABLE console_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='console',
    geaflow.dsl.console.skip = true
)
```
## 参数

| 参数名 | 是否必须 | 描述                     |
| -------- | -------- |------------------------|
| geaflow.dsl.console.skip     | 否     | 是否跳过日志，即无任何输出，默认为false |

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