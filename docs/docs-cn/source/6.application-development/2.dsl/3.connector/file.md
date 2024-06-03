# File Connector介绍
GeaFlow 支持从文件中读取数据，也支持向文件写入数据。
## 语法

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
## 参数

| 参数名 | 是否必须 | 描述                           |
| -------- |------|------------------------------|
| geaflow.file.persistent.config.json     | 否    | JSON格式的DFS配置，会覆盖系统环境配置。      |
| geaflow.dsl.file.path     | 是    | 读取或写入的文件或文件夹的路径              |
| geaflow.dsl.column.separator     | 否    | 用于将文本分割为列的列分隔符。默认值为英文逗号','。  |
| geaflow.dsl.line.separator     | 否    | 用于将文本分割为列的行分隔符。默认值为换行符'\n'。  |
| geaflow.dsl.file.name.regex    | 否    | 读取文件名称正则过滤规则，默认为空。           |
| geaflow.dsl.file.format     | 否    | 读写文件格式，支持parquet、txt，默认为txt。 |

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