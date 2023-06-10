CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	e.weight
FROM (
  MATCH(a:person where a.id = 1)-[e]->{1, 2}(b)
  RETURN a, e, b
)
;

