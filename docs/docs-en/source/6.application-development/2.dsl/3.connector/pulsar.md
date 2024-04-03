# Pulsar Connector Introduction
GeaFlow supports reading data from Pulsar and writing data to Pulsar. The currently supported Pulsar version is 2.8.1.
## Syntax
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
## Options

| Key | Required | Description |
| -------- | -------- | -------- |
| geaflow.dsl.pulsar.servers     | yes    | The pulsar bootstrap servers list.      |
| geaflow.dsl.pulsar.port     | yes     | The port of pulsar bootstrap servers.      |
| geaflow.dsl.pulsar.topic     | yes     | Pulsar topic|
| geaflow.dsl.pulsar.subscriptionInitialPosition     | No     | The initial position of consumer, default is 'latest'.|

Note: Pulsar connector cannot specify a partition topic. If you want to consume messages for a certain partition, please select the sub topic name of the partition topic.
## Example1
Example 1 is from pulsar to `topic_read` data and write it to the `topic_write`.
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
## Example2
Similarly, we can also perform a fourth degree loop detection.
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
