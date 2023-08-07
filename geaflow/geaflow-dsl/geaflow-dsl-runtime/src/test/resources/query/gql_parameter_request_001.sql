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
	a_id,
	b_id,
	weight
FROM (
  WITH p AS (
    SELECT * FROM (VALUES(1, 0.4), (4, 0.5)) AS t(id, weight)
  )
  MATCH (a:person where a.id = p.id) -[e where weight > p.weight + 0.1]->(b)
  RETURN a.id as a_id, e.weight as weight, b.id as b_id
)