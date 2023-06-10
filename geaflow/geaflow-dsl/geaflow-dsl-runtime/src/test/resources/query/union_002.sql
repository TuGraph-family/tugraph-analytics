-- union test
CREATE TABLE users (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar
)WITH(
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users2.txt'
);

CREATE TABLE users_en (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar
)WITH(
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users2.txt'
);

CREATE TABLE users_zh (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar
)WITH(
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users2.txt'
);

CREATE TABLE output_console (
	rt bigint,
	f1 bigint,
	f2 double,
	f3 varchar
)WITH(
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO output_console
SELECT * FROM (
  SELECT * FROM users AS t1 WHERE t1.f1 < 20
  UNION
  SELECT * FROM users_en AS t2 WHERE t2.f1 > 15
  UNION ALL
  SELECT * FROM users_zh AS t3 WHERE t3.f1 > 20
) ORDER BY rt, f1, f2, f3
;