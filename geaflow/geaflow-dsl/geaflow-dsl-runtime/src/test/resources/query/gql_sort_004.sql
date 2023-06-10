CREATE TABLE tbl_result (
  a_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id
FROM (
  MATCH (a) order by a.id % 2 DESC, a.id
  RETURN a
)
;

