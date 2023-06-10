CREATE TABLE tbl_result (
   srcId bigint,
   targetId bigint,
   weight double,
   ratio double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	e.srcId,
	e.targetId,
	e.weight,
	e.ratio
FROM (
  MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
  LET e.ratio = e.weight * a.age / b.age
)