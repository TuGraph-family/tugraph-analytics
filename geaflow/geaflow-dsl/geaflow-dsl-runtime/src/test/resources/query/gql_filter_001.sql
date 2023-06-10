CREATE TABLE tbl_result (
  a_id bigint,
  weight double,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	e.weight,
	b_id
FROM (
  match(a:person where a.id = 1)-[e:knows]->(b:person)
  RETURN a.id AS a_id, e, b.id AS b_id
  THEN FILTER b_id = 4 AND e.weight > 0
)

