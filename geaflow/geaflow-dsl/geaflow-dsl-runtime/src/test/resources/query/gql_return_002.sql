CREATE TABLE tbl_result (
  b_id bigint,
  name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	b_id,
	name
FROM (
  MATCH (a:person WHERE a.id = 1)-[e]->(b:person|software)
  RETURN b.id as b_id, b.name as name order by name DESC
)
