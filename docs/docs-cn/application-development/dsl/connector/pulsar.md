# Pulsar Connector介绍
GeaFlow 支持从 Pulsar 中读取数据，并向 Pulsar 写入数据。目前支持的 Pulsar 版本为 2.8.1。
# 语法
```sql
CREATE TABLE pulsar_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='pulsar',
    `geaflow.dsl.pulsar.servers` = 'localhost',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_read',
    `geaflow.dsl.pulsar.subscriptionInitialPosition` = 'latest'
)
```
# 参数

| 参数名 | 是否必须 | 描述 |
| -------- | -------- | -------- |
| geaflow.dsl.pulsar.servers     | 是     | Pulsar 的引导服务器（bootstrap）列表     |
| geaflow.dsl.pulsar.port     | 是     | Pulsar 的引导服务器（bootstrap）端口号     |
| geaflow.dsl.pulsar.topic     | 是     | Pulsar topic|
| geaflow.dsl.pulsar.subscriptionInitialPosition     | 否     | Pulsar消费的初始位置，默认是 'latest', 可选择 'earliest‘|

注意：pulsar connector不能指定一个分区topic， 如果你要消费某个分区的消息，请选择分区topic的子topic名称。

# 示例1
示例1是从pulsar从`topic_read`中读取数据并且将数据写入`topic_write`中。
```sql
CREATE TABLE pulsar_source (
    id BIGINT,
    name VARCHAR,
    age INT
) WITH (
    type='pulsar',
    `geaflow.dsl.pulsar.servers` = 'localhost',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_read',
    `geaflow.dsl.pulsar.subscriptionInitialPosition` = 'latest'
    );
CREATE TABLE pulsar_sink (
    id BIGINT,
    name VARCHAR,
    age INT
) WITH (
    type='pulsar',
    `geaflow.dsl.pulsar.servers` = 'localhost',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_write'
    );
INSERT INTO pulsar_sink
SELECT * FROM pulsar_source;
```
# 示例2
同样我们也可以进行四度环路检测。
```sql
set geaflow.dsl.window.size = 1;
set geaflow.dsl.ignore.exception = true;

CREATE GRAPH IF NOT EXISTS pulsar_modern (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS pulsar_source (
    text varchar
) WITH (
    type='pulsar',
    `geaflow.dsl.column.separator` = '#',
    `geaflow.dsl.pulsar.servers` = 'localhost',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_read',
    `geaflow.dsl.pulsar.subscriptionInitialPosition` = 'latest'
    );

CREATE TABLE IF NOT EXISTS pulsar_sink (
    a_id bigint,
    b_id bigint,
    c_id bigint,
    d_id bigint,
    a1_id bigint
) WITH (
    type='pulsar',
    `geaflow.dsl.pulsar.servers` = 'localhost',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_write'
    );

USE GRAPH pulsar_modern;

INSERT INTO pulsar_modern.person(id, name)
SELECT
    cast(trim(split_ex(t1, ',', 0)) as bigint),
    split_ex(trim(t1), ',', 1)
FROM (
    Select trim(substr(text, 2)) as t1
    FROM pulsar_source
    WHERE substr(text, 1, 1) = '.'
    );

INSERT INTO pulsar_modern.knows
SELECT
    cast(split_ex(t1, ',', 0) as bigint),
    cast(split_ex(t1, ',', 1) as bigint),
    cast(split_ex(t1, ',', 2) as double)
FROM (
    Select trim(substr(text, 2)) as t1
    FROM pulsar_source
    WHERE substr(text, 1, 1) = '-'
    );

INSERT INTO pulsar_sink
SELECT
    a_id,
    b_id,
    c_id,
    d_id,
    a1_id
FROM (
      MATCH (a:person) -[:knows]->(b:person) -[:knows]-> (c:person)
          -[:knows]-> (d:person) -> (a:person)
          RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id, a.id as a1_id
    );
```
