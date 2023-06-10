CREATE TABLE tbl_result (
	user_name varchar,
	user_count bigint
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'sink-test'
);

INSERT INTO tbl_result VALUES ('json', 111)
;
