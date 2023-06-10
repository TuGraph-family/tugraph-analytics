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
	_b.id
FROM (
  MATCH (a:person WHERE id = 1)-[e:knows]->(b:person)
  RETURN a as _a, e as _e, b as _b
  group by _a, _e, _b order by _a, _b
)
