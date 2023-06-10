CREATE TABLE tbl_result (
  a_id bigint,
  e_weight double,
  amt bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	e_weight,
	amt
FROM (
  MATCH (a:person WHERE id = 1)-[e:knows]->(b:person)
  RETURN a.id as a_id, e.weight as e_weight, b.age * 10 as amt
)