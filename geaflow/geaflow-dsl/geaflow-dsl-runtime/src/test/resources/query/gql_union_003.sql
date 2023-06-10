CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
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
	d.id
FROM (
  MATCH(a:person where a.id = 4)-[e:created]->(b) |+| (a:person where a.id = 2) <-[:knows]-(d)
  RETURN a, b, d
)
;

