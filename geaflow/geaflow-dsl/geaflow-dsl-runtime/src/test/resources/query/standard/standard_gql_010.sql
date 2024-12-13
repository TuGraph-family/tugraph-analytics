CREATE TABLE tbl_result (
  f1 varchar,f2 bigint,f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH movie_graph;

INSERT INTO tbl_result
MATCH (n:person {id:1,name:'Andres'}) where n.born != 1000 RETURN n.name, n.born, n.id;