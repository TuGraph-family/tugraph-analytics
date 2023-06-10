CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint,
  p_id bigint
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
	p.id
FROM (
  MATCH
       (a:person where a.id = 4)-[e:created]->(b),
       (c:person where id = 1) -[:knows]-> (d where d.id = 2),
       (a:person) <-[:knows]- (p)
  RETURN a, b, c, d, p
)
;

