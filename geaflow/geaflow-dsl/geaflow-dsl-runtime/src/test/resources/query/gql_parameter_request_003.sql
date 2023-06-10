CREATE TABLE tbl_result (
  age int,
  cnt bigint,
  ratio double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	age,
	cnt,
	ratio
FROM (
  WITH p AS (
    MATCH (a) RETURN CAST(COUNT(a) AS DOUBLE) as cnt
  )
  MATCH (a)
  RETURN a.age as age, COUNT(a) as cnt, COUNT(a) / p.cnt as ratio GROUP BY a.age, p.cnt
)