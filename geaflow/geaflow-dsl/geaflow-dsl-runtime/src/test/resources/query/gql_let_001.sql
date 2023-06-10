CREATE TABLE tbl_result (
  a_id bigint,
  a_weight double,
  b_id bigint,
  b_weight double
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
	b_weight
FROM (
  MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
  LET a.weight = a.age / cast(100.0 as double),
  LET b.weight = b.age / cast(100.0 as double)

  RETURN a.id as a_id, a.weight as a_weight, b.id as b_id,b.weight as b_weight
)