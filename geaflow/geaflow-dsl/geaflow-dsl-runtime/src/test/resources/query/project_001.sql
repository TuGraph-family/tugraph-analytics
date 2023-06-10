USE INSTANCE `default`;

CREATE TABLE IF NOT EXISTS users2 (
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
SELECT
	f1,
	f2,
	trim(f3)
FROM `default`.users2;