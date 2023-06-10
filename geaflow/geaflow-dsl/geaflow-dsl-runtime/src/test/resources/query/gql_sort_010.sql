CREATE TABLE tbl_result (
  a_id bigint,
  d_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	d_id
FROM (
  SELECT 999 AS a_id, 888 as d_id
  UNION ALL
  MATCH (a where id in (4))-(b)-(c)-(d) limit 0
  RETURN a.id as a_id, d.id as d_id
)
;

