create function mycount as 'com.antgroup.geaflow.dsl.runtime.query.udf.MyCount';
create function concat as 'com.antgroup.geaflow.dsl.udf.table.string.Concat';

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
  concat(o.name, '-' , concat(o.name, '-', o.name)) as _name,
  mycount(o.name, o.id) AS count_id,
  SUM(o.id) AS sum_id,
  MAX(o.id) AS max_id,
  MIN(o.id) AS min_id,
  AVG(o.id) AS avg_id,
  COUNT(DISTINCT o.id) AS distinct_id
FROM users o
GROUP BY _name;
