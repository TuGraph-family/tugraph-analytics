CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);
USE GRAPH modern;
INSERT INTO tbl_result
SELECT
	a.id,
	b.id
FROM (
  MATCH (a)<-(b where id = 1)
  Where COUNT((b)->(c) => if (c.id > a.id, 0, cast(null as int))) > 0
  RETURN a, b
)