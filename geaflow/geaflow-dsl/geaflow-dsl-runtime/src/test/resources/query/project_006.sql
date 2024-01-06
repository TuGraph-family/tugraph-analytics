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
	id bigint,
	console_f2 double,
	console_f3 varchar,
	PRIMARY KEY(id)
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT ALL
  f1,
  f2,
  f3
FROM users;
