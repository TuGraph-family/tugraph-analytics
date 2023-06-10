CREATE TABLE tbl_result (
  f0 bigint,
  f1 double,
  f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	_a.id,
	_e.weight,
	b_id
FROM (
  MATCH (a:person WHERE a.id = 1)-[e]->(b:person | software)
  RETURN a as _a, e as _e, b.id as b_id
  Order by e DESC, a
  Limit 10
)
