CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
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
	f.id
FROM (
  MATCH
       (a:person where a.id = 4)-[]->(b where b.id = 3)
       |+| (c:person where c.id = 4)-[]->(b where b.id = 5)
       , (f where f.id = 6) -[:created]-> (b)
  RETURN a, b, c, f
)
;

