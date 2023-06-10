CREATE TABLE console (
  vid int,
	distance bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO console
CALL SSSP(1) YIELD (vid, distance)
RETURN cast (vid as int), distance
;