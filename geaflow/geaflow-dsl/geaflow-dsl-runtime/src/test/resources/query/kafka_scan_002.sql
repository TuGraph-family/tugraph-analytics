CREATE TABLE kafka_source (
	id bigint,
	name varchar,
	age long
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'scan_002',
	geaflow.dsl.kafka.data.operation.timeout.seconds = 5,
	geaflow.dsl.time.window.size=10,
	geaflow.dsl.start.time='${startTime}'
);

CREATE TABLE tbl_result (
	id bigint,
	name varchar,
	age long
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT DISTINCT id, name, age
FROM kafka_source
ORDER BY id
LIMIT 5
;
