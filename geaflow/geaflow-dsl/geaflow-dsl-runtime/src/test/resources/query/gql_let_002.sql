CREATE TABLE tbl_result (
  a_id bigint,
  a_weight double,
  b_id bigint,
  b_weight double,
  b_age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	a_weight,
	b_id,
	b_weight,
	b_age
FROM (
  MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
  LET a.weight = a.age / cast(100.0 as double),
  LET a.weight = a.weight * 2,
  LET b.weight = 1.0,
  LET b.age = 20
  RETURN a.id as a_id, a.weight as a_weight, b.id as b_id,b.weight as b_weight, b.age as b_age
)