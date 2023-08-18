CREATE TABLE users (
	id long,
	name string,
	age double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_double.txt'
);

CREATE TABLE tbl_result (
	user_name varchar,
	count_id bigint,
	sum_id bigint,
	max_id bigint,
	min_id bigint,
	avg_id DOUBLE,
	distinct_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
  o.name,
  COUNT(o.id)*2 AS count_id,
  SUM(o.id)*2 AS sum_id,
  MAX(o.id)*2 AS max_id,
  MIN(o.id)*2 AS min_id,
  AVG(o.id)*2 AS avg_id,
  COUNT(DISTINCT o.id) AS distinct_id
FROM users o
GROUP BY o.name;
