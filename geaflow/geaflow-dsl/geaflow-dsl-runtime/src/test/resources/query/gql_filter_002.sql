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
  RETURN a.id as a_id, e, b.id as b_id
  THEN FILTER a_id = 1 OR a_id = 2 AND CAST(e.weight as int) = 2
)


