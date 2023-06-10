CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  age_sum bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	age_sum
FROM (
  MATCH (a)-[e]-(b) order by a.age + b.age DESC
  RETURN a, e, b, a.age + b.age as age_sum
)
;

