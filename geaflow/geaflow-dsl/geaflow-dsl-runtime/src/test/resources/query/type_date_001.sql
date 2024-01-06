CREATE TABLE IF NOT EXISTS users3 (
	rt TIMESTAMP,
	f1 bigint,
	f2 double,
	f3 varchar,
	f4 boolean,
	f5 date
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users3.txt'
);

CREATE TABLE console(
	f5 date,
	cnt bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
SELECT
	f5,
	count(f1)
FROM users3
group by f5
;