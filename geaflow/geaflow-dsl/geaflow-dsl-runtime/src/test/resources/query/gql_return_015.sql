CREATE TABLE tbl_result (
  f0 bigint,
  f1 double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	_a.id,
	_e.weight
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  RETURN a as _a, e as _e, b.id as b_id
  group by b.id, e, a order by e
)
