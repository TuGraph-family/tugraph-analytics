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
MATCH (a:person where id = 1)-[e:knows]-(b:person) WHERE a.age < b.age
RETURN a.id as a_id, e.weight as weight, b.id as b_id
)