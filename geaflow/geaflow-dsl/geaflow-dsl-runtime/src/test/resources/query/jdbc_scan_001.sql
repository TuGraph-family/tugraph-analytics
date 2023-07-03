CREATE TABLE jdbc_source (
	user_name varchar,
	user_count bigint
) WITH (
	type='jdbc',
	geaflow.dsl.jdbc.driver = 'org.h2.Driver',
	geaflow.dsl.jdbc.url = 'jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1',
	geaflow.dsl.jdbc.username = 'h2_user',
	geaflow.dsl.jdbc.password = 'h2_pwd',
	geaflow.dsl.jdbc.table.name = 'test'
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
FROM jdbc_source;
