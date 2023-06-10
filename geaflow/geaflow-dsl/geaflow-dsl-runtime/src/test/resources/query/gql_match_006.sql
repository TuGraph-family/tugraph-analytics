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
	weight,
	b_id
FROM (
Match (a:person WHERE id = 1 and name like 'marko')-[e where weight > 0]->(b:person where b.age > 25)
RETURN a.id as a_id, e.weight as weight, b.id as b_id
)
