CREATE TABLE tbl_result (
  vid int,
	distance bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
CALL SSSP(1) YIELD (vid, distance)
RETURN cast (vid as int), distance
;