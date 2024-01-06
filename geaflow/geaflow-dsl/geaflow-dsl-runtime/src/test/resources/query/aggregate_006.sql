CREATE TABLE users (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_duplication.txt'
);

CREATE TABLE tbl_result (
  name varchar,
	cnt long
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
select
  groupName, sum(cnt)
FROM (
  select id, name as groupName, count(1) as cnt from users u  group by id, groupName
) a
GROUP BY groupName
;