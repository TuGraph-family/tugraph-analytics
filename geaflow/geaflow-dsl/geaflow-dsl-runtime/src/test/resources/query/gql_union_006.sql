CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint,
  f_id bigint,
  g_id bigint
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
	f.id,
	g.id
FROM (
  MATCH
       (a:person where a.id = 4)-[e:created]->(b)
       | (c:person where c.id = 4)-[]->(d)<-()
       | (f:person where f.id = 4) -[]- (g)
  RETURN a, b, c, d, f, g
)
;

