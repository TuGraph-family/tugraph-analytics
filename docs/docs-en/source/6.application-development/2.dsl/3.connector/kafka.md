# Kafka Connector Introduction
GeaFlow support read data from kafka and write data to kafka. Currently support kafka version is 2.4.1.
# Syntax

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
# Options

| Key | Required | Description |
| -------- | -------- | -------- |
| geaflow.dsl.kafka.servers     | true     | The kafka bootstrap servers list.     |
| geaflow.dsl.kafka.topic     | true     | The kafka topic.|
| geaflow.dsl.kafka.group.id     | false     | The kafka group id. Default value is: 'default-group-id'.|


# Example

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