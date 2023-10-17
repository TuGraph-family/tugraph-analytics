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
	console_f1 bigint,
	console_f2 double,
	console_f3 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT DISTINCT
	f1,
	f2,
	if (f3 is not null, f3, null)
FROM users;
