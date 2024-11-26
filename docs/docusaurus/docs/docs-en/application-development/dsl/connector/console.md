# Console Connector Introduction

# Syntax

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
# Options

| Key | Required | Description |
| -------- | -------- |------------------------|
| geaflow.dsl.console.skip     | false     | Whether to skip the log, i.e., no output at all. The default value is false. |

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