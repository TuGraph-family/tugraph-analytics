CREATE TABLE users (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users.txt'
);

CREATE TABLE console (
  cnt int,
	sum_age int,
	avg_age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO console
select
  sum(cnt), sum(age), avg(age)
FROM (
  select id, age, count(1) as cnt from users u group by id, age
) a
;