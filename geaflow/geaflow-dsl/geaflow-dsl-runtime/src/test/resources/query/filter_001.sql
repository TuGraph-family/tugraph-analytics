CREATE TABLE users (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users.txt',
	geaflow.dsl.file.name.regex = '^users.*'
);

CREATE TABLE console (
	id bigint,
  age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
SELECT id, age FROM users
WHERE age > 20;