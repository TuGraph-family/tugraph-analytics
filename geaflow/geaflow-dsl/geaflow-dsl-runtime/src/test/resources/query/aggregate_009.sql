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
  name,
  COUNT(id) AS count_id,
  SUM(id) AS sum_id,
  MAX(id) AS max_id,
  MIN(id) AS min_id,
  AVG(id) AS avg_id,
  COUNT(DISTINCT id) AS distinct_id
FROM users o
GROUP BY name;
