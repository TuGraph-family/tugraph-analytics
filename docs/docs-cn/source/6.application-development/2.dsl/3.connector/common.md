# Connector基础介绍
GeaFlow 支持从各类connector中读写数据，GeaFlow将它们都识别为外部表，并将元数据存储在Catalog中。

# 语法

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

声明一张表来使用Connector，具体使用Source/Sink由读写行为决定。

TEMPORARY 用于创建临时表，临时表不写入Catalog。如果不指定 IF NOT EXISTS 则会在存在同名表时报错。

WITH子句用于指定Connector的配置信息，其中的type字段必须，用来指定使用Connector类型，例如file表示使用文件。

同时我们可以在WITH中添加表的参数，这些参数会覆盖掉外部(SQL文件中、作业参数中)的配置项，具有最高优先级。

# 主要参数

| 参数名                          | 是否必须 | 描述                                                                                                            |
|------------------------------| -------- |---------------------------------------------------------------------------------------------------------------|
| type                         | 是     | 指定使用的Connector类型                                                                                              |
| geaflow.dsl.window.size | 否     | 重要，-1表示读取所有数据为一个窗口，属于批式处理。正数表示若干条数据为一个窗口，为流式处理。                                                               |
| geaflow.dsl.partitions.per.source.parallelism | 否     | 将Source的分片若干个编为一组，减少并发数关联的资源使用量。      |


# 示例

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