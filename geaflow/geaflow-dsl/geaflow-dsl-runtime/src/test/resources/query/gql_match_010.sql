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
	a_id,
	b_id,
	c_id
FROM (
  MATCH (a) -> (b) - (c) -> (a)
  RETURN a.id as a_id, b.id as b_id, c.id as c_id
)