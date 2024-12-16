CREATE TABLE tbl_result (
  f1 double,  f2 bigint,  f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH movie_graph;

INSERT INTO tbl_result
match(a:person) return avg(a.id), max(a.id), a.born group by a.born