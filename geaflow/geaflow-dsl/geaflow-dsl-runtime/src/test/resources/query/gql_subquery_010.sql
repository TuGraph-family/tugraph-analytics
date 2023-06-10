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
  MATCH (a:person)->(b:software)
  Where MAX((b:software)-(c:person) => c.age) > a.age
  RETURN a, b order by a
)