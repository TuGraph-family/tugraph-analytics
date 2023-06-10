CREATE TABLE kafka_source (
	user_name varchar,
	user_count bigint
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'sink-test'
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
