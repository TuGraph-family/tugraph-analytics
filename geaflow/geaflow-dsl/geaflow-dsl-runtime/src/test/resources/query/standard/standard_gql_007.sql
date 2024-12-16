CREATE TABLE tbl_result (
  f1 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH movie_graph;

INSERT INTO tbl_result
MATCH (a:person) RETURN a.id offset 18 limit 5