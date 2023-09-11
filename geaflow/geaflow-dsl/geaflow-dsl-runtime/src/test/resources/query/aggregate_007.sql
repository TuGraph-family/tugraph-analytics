CREATE TABLE users (
	id long,
	name string,
	age double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_double.txt'
);

CREATE TABLE tbl_result (
	max_age double,
	min_age double,
	max_c char,
	min_c char
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
select
  max(age),  min(age), max(cnt),  min(cnt)
FROM (
  select id, name, age, '1' as cnt from users u
) a
;