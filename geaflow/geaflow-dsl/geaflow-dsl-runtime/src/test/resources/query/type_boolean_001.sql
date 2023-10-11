CREATE TABLE IF NOT EXISTS users3 (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar,
	f4 boolean,
	f5 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users3.txt'
);

CREATE TABLE console(
	f4 boolean,
	cnt bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
SELECT
	f4,
	count(f1)
FROM users3
group by f4
;