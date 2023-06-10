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
       (a:person where a.id = 4)-(b),
       (b)->(c) |+| (b)<-(c),
       (c)->(f) |+| (c)<-(d)<-(f)
  order by f, a, b, c, d limit 10
  RETURN DISTINCT a, b, c, d, f
)
;
