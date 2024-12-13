CREATE TABLE tbl_result (
  f1 bigint,f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH movie_graph;

INSERT INTO tbl_result
CALL SSSP(1) YIELD (id, dst) RETURN id, dst