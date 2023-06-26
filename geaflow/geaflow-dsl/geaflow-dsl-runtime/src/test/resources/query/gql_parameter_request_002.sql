CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  weight double,
  name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	b_id,
	weight,
	name
FROM (
  WITH p AS (
    SELECT * FROM (VALUES(1, 'r0', 0.4), (4, 'r1', 0.5)) AS t(id, name, weight)
  )
  MATCH (a:person) -[e where weight > p.weight]->(b)
  RETURN p.name as name, a.id as a_id, e.weight as weight, b.id as b_id
)