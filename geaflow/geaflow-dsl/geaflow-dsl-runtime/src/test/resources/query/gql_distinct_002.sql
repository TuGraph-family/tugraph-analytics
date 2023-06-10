CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id
FROM (
  MATCH (a:person where a.id = 4)-[e]->(b) |+| (a:person where a.id = 4) -[e]- (b)
  RETURN DISTINCT a, b
)
;

