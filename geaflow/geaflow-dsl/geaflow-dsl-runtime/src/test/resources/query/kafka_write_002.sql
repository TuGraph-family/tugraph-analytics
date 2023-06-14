set geaflow.dsl.window.size = -1;
set geaflow.dsl.custom.sink.`function` = 'com.antgroup.geaflow.dsl.runtime.testenv.FoGeaFlowTableSinkFunction';

CREATE TABLE users (
	id bigint,
	name varchar,
	age long
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users.txt'
);

CREATE TABLE tbl_result (
	id bigint,
	name varchar,
	age long
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'fo-test'
);

INSERT INTO tbl_result
SELECT * FROM users
;
