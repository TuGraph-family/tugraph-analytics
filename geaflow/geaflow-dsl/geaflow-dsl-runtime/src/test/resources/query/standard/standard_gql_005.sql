CREATE TABLE tbl_result (
  f1 bigint,  f2 varchar,  f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH movie_graph;

INSERT INTO tbl_result
MATCH (a:person) RETURN a.id as a_id, a.name,a.born ORDER BY a_id ASC limit 10