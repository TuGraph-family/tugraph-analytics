CREATE TABLE users (
	id bigint,
	name varchar,
	age double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_double.txt'
);

CREATE TABLE console (
  cnt int,
	sum_age double,
	avg_age double
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