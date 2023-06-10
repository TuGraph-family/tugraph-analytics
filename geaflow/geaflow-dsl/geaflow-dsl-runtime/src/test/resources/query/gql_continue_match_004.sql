CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  c_neighbours bigint,
  d_id bigint,
  d_type int
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
	c.neighbours,
	d.id,
	d.type
FROM (
  MATCH (a) -> (b) where b.id > 0 and a.lang is null
  MATCH (a) <- (c) where label(c) = 'person'
  Let c.neighbours = COUNT((c) -> (d) => d.id)
  MATCH (c) -> (d) where d != b
  Let d.type = if (label(d) = 'person', 1, 0)
  RETURN a, b, c, d
)
;

