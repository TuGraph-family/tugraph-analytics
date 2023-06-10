CREATE TABLE tbl_result (
  f0 bigint,
  f1 double,
  f2 double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	_a.id,
	_e.weight,
	weight
FROM (
  MATCH (a:person WHERE a.id = 1)-[e:knows]->(b:software|person)
  RETURN a as _a, e as _e, weight
  Order by _e DESC, _a DESC
  Limit 10
)
