CREATE TABLE tbl_result (
  name varchar,
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	name,
	_e.srcId,
	_e.targetId
FROM (
  MATCH (a:person WHERE a.id = 4)-()-()-[e]-(b)
  RETURN b.name, e as _e
  THEN FILTER (name = "josh" OR name = "marko") AND _e.weight >= 0.5
)

