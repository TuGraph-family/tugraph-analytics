CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	b_id,
	c_id,
	d_id
FROM (
  WITH p AS (
    SELECT * FROM (VALUES(4, 2), (3, 1)) AS t(id1, id2)
  )
  MATCH (a:person where a.id = p.id1)-[e:created]->(b), (c:person where id = p.id2) <-[:knows]- (d)
  RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id
)
;
