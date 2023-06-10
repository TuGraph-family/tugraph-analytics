CREATE TABLE tbl_result (
  a_id bigint,
  weight double,
  f_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	weight,
	f_id
FROM (
MATCH (a:person WHERE a.id = 1)-[]->()->(c:software)<-[e:created]-(f:person)
RETURN a.id as a_id, e.weight, f.id as f_id
)