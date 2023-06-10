CREATE TABLE tbl_result (
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	b.id
FROM (
  MATCH(a:person where a.id = 1)-[e]->{0, 2}(b)
  RETURN b
)
;

