CREATE TABLE users (
	id long,
	name string,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_duplication.txt',
	geaflow.dsl.window.size = 1
);

CREATE TABLE tbl_result (
  max_id long,
	max_name string,
	max_age int,
  min_id long,
  min_name string,
  min_age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
select
  max(id), max(name), max(age), min(id), min(name), min(age)
FROM users
;