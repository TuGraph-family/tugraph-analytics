CREATE TABLE tbl_result (
  name varchar,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	name,
	_b.id
FROM (
  MATCH (a:person WHERE a.id = 1)-[e:knows]->(b:person)
  RETURN b.name as name, b as _b
  THEN FILTER name IS NOT NULL AND _b.id > 1.5
)

