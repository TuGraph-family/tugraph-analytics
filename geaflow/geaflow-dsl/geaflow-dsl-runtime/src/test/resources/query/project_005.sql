CREATE TABLE users (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users2.txt'
);

CREATE TABLE output_console(
  rt bigint,
	console_f1 bigint,
	console_f2 double,
	console_f3 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT ALL
  *
FROM users;
