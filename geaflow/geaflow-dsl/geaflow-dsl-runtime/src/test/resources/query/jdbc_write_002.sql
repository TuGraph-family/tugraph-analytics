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
	type='jdbc',
	geaflow.dsl.jdbc.driver = 'org.h2.Driver',
	geaflow.dsl.jdbc.url = 'jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1',
	geaflow.dsl.jdbc.username = 'h2_user',
	geaflow.dsl.jdbc.password = 'h2_pwd',
	geaflow.dsl.jdbc.table.name = 'users'
);

INSERT INTO tbl_result
SELECT * FROM users
;