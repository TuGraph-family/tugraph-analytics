# Hudi Connector Introduction
GeaFlow Hudi currently supports reading data from files.
# Syntax

```sql
CREATE TABLE IF NOT EXISTS hudi_person (
  id BIGINT,
  name VARCHAR
) WITH (
  type='hudi', 
  geaflow.file.persistent.config.json = '{\'fs.defaultFS\':\'namenode:9000\'}',
  geaflow.dsl.file.path='/path/to/hudi_person'
);
```
# Options

| Key | Required | Description |
| -------- | -------- | -------- |
| geaflow.dsl.file.path     | true     | The path of the file or folder to read from or write to.     |
| geaflow.file.persistent.config.json     | false    | JSON-formatted DFS configuration, which will override the system environment configuration.      |

# Example

```sql
set geaflow.dsl.window.size = -1;

CREATE TABLE IF NOT EXISTS hudi_person (
  id BIGINT,
  name VARCHAR
) WITH (
   type='hudi', 
  `geaflow.file.persistent.config.json` = '{\'fs.defaultFS\':\'namenode:9000\'}',
	geaflow.dsl.file.path='/path/to/hudi_person'
);

CREATE TABLE IF NOT EXISTS hudi_sink (
  id BIGINT,
  name VARCHAR
) WITH (
  type='hudi', 
  `geaflow.file.persistent.config.json` = '{\'fs.defaultFS\':\'namenode:9000\'}',
	geaflow.dsl.file.path='/path/to/hudi_sink'
);

INSERT INTO hudi_sink
SELECT * FROM hudi_person;
```