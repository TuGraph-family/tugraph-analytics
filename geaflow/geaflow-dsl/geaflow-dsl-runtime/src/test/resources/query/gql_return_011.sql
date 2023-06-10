CREATE TABLE tbl_result (
  f0 bigint,
  f1 bigint,
  f2 double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	_a,
	_e,
	_w
FROM (
  MATCH (a:person WHERE a.id = 1)-[e]->(b:software |person)
  RETURN MIN(a.id) as _a, MAX(e.targetId) as _e, AVG(weight) as _w
  group by a, e
  Order by a DESC, e DESC
  Limit 10
)
