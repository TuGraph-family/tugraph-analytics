CREATE TABLE tbl_result (
  f0 bigint,
  f1 bigint,
  f2 bigint,
  f3 bigint,
  f4 double,
  f5 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_name,
	e_weight,
	b_age_max,
	b_age_min,
	b_age_avg,
	_b.id
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  RETURN COUNT(a.name) as a_name, SUM(e.weight) as e_weight,
  MAX(b.age) as b_age_max, MIN(b.age) as b_age_min,
  AVG(b.age) as b_age_avg, b as _b
  group by _b order by _b
)
