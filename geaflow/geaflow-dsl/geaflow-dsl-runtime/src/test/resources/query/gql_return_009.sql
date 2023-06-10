CREATE TABLE tbl_result (
  b_id bigint,
  amt double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	b.id,
	amt
FROM (
  MATCH (a:person WHERE a.id = 4)-()-(b)-[e]-(c)
  RETURN b, SUM(e.weight * 10) as amt GROUP BY b ORDER BY b
)
