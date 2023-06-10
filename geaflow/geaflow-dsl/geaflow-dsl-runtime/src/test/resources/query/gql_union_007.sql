CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint,
  f_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
  c.id,
  d.id,
	f.id
FROM (
  MATCH
       (a where a.id = 4)
       | (b where b.id = 4)
       |+| (c where c.id = 4)
       | (d where d.id = 4)
       |+| (f where f.id = 4)
  RETURN a, b, c, d, f
)
;

