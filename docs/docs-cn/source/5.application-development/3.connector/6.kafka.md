# Kafka Connector介绍
GeaFlow 支持从 Kafka 中读取数据，并向 Kafka 写入数据。目前支持的 Kafka 版本为 2.4.1。
## 语法

```sql
CREATE TABLE kafka_table (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='kafka',
    geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'test-topic'
)
```
## 参数

| 参数名 | 是否必须 | 描述 |
| -------- | -------- | -------- |
| geaflow.dsl.kafka.servers     | 是     | Kafka 的引导服务器（bootstrap）列表     |
| geaflow.dsl.kafka.topic     | 是     | Kafka topic|
| geaflow.dsl.kafka.group.id     | 否     | Kafka组（group id），默认是'default-group-id'.|


## 示例

```sql
CREATE TABLE kafka_source (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='kafka',
    geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'read-topic'
);

CREATE TABLE kafka_sink (
  id BIGINT,
  name VARCHAR,
  age INT
) WITH (
	type='kafka',
    geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'write-topic'
);

INSERT INTO kafka_sink
SELECT * FROM kafka_source;
```