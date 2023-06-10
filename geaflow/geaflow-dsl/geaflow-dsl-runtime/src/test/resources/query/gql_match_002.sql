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
MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
RETURN a.id as a_id, e.weight as weight, b.id as b_id;
