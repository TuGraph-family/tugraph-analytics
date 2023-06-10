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
	a.id,
	b.id,
	c.id,
	d.id
FROM (
  MATCH(a:person where a.id = 4)-[e]->(b) | (c:person where c.id = 4) -[]- (d)
  RETURN a, b, c, d
)
;

