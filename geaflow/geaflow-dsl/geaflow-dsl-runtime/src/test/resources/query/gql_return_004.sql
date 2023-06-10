CREATE TABLE tbl_result (
  b_id bigint,
  e_weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	b_id,
	e_weight
FROM (
  MATCH (a:person WHERE a.id = 4)-()-()-[e]-(b)
  RETURN b.id as b_id, e.weight as e_weight Order by e_weight DESC, b.id Limit 5
)