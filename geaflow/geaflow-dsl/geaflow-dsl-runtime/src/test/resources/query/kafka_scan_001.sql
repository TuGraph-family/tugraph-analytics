CREATE TABLE kafka_source (
	user_name varchar,
	user_count bigint
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'sink-test',
  `geaflow.dsl.start.time` = '2025-10-01 09:00:00'
);

CREATE TABLE tbl_result (
	user_name varchar,
	user_count bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT DISTINCT *
FROM kafka_source;
