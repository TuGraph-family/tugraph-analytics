CREATE TABLE tbl_result (
  a_id bigint,
  weight double,
  b_id bigint,
  b_sum_weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	e.weight,
	b.id,
	b.sum_weight
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  LET b.sum_weight = SUM((b) => e.weight)
  RETURN a, e, b
)