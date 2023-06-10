CREATE TABLE tbl_result (
  a_id bigint,
  c_id bigint,
  d_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	c.id,
	d.id
FROM (
  MATCH (a where id in (4))-(b)-(c)-(d) order by c.id DESC, d.id
  RETURN a, c, d
)
;

