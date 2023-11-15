CREATE GRAPH using_modern (
	Vertex person (
	  id bigint ID,
	  name varchar,
	  age int
	),
	Vertex software (
	  id bigint ID,
	  name varchar,
	  lang varchar
	),
	Edge knows (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID,
	  weight double
	),
	Edge created (
	  srcId bigint SOURCE ID,
  	targetId bigint DESTINATION ID,
  	weight double
	)
) WITH (
	storeType='rocksdb',
	shardCount = 2
);


CREATE TABLE tbl_result (
  vid int,
	distance bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH using_modern;

INSERT INTO tbl_result
CALL SSSP(1) YIELD (vid, distance)
RETURN cast (vid as int), distance
;