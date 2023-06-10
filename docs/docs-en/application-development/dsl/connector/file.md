# File Connector Introduction
GeaFlow support read data from file and write data to file.
# Syntax

```sql
CREATE TABLE file_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='file',
    geaflow.dsl.file.path = '/path/to/file'
)
```
# Options

| Key | Required | Description |
| -------- | -------- | -------- |
| geaflow.dsl.file.path     | true     | The file path to read or write.     |
| geaflow.dsl.column.separator     | false     | The column separator for split text to columns.Default value is ','.     |
| geaflow.dsl.line.separator     | false     | The line separator for split text to columns..Default value is '\n'.     |


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

CREATE TABLE file_sink (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='file',
    geaflow.dsl.file.path = '/path/to/file'
);

INSERT INTO file_sink
SELECT * FROM file_source;
```