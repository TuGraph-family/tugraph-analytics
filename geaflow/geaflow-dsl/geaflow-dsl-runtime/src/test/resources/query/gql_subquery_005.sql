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
	a.id,
	e.weight,
	b.id
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  Where EXISTS (b) -> (c)
       And SUM((b) -[e1]-> (c) => e1.weight) > 1
  RETURN a, e, b
)