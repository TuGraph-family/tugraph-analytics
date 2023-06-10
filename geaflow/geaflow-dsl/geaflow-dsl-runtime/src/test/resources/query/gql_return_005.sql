CREATE TABLE tbl_result (
  a_id bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	_b.id,
	_e.weight + 1000
FROM (
  MATCH (a:person WHERE a.id = 4)-()-()-[e]-(b)
  RETURN b as _b, e as _e Order by _b.id DESC, _e.weight + 1000
  Limit 5
)