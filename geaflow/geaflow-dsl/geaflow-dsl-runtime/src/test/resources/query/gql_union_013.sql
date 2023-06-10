CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	e.srcId,
	e.targetId
FROM (
  MATCH(a:person where a.id = 4)-[e]->(b) | (a:person where a.id = 4)-[e]->(b)
  RETURN a, e
)
order by a.id, e.srcId, e.targetId
;

