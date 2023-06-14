set geaflow.dsl.window.size = -1;
set geaflow.dsl.custom.sink.`function` = 'com.antgroup.geaflow.dsl.runtime.testenv.FoGeaFlowTableSinkFunction';

CREATE TABLE kafka_source (
	id bigint,
	name varchar,
	age long
) WITH (
	type='kafka',
	geaflow.dsl.kafka.servers = 'localhost:9092',
	geaflow.dsl.kafka.topic = 'fo-test'
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
