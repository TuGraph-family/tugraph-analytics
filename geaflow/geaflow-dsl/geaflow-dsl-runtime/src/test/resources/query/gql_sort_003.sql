CREATE TABLE tbl_result (
  a_id bigint,
  e_src_id bigint,
  e_target_id bigint,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	e.srcId,
	e.targetId,
	b.id
FROM (
  MATCH (a)-[e]-(b) order by e.srcId DESC, e Asc limit 10
  RETURN a, e, b
)
;

