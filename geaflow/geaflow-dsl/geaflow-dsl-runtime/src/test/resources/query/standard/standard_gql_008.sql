CREATE TABLE tbl_result (
  f1 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH movie_graph;

INSERT INTO tbl_result
MATCH (a:person where a.id = 1)-[e:know]->(b:person) RETURN b.id offset 1 limit 1