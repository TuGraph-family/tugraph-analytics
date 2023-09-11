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
| geaflow.file.persistent.config.json     | false    | The JSON format DFS configuration will override the system environment configuration.      |
| geaflow.dsl.file.path     | true     | The file path to read or write.     |
| geaflow.dsl.column.separator     | false     | The column separator for split text to columns.Default value is ','.     |
| geaflow.dsl.line.separator     | false     | The line separator for split text to columns..Default value is '\n'.     |
| geaflow.dsl.file.name.regex    | false    | The regular expression filter rule for file name reading is empty by default.           |
| geaflow.dsl.file.format     | false    | The file format for reading and writing supports Parquet and TXT, with the default format being TXT. |

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