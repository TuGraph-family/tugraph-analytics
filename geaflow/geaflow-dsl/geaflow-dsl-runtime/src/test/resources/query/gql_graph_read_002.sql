CREATE GRAPH modern_2 (
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
	shard.count = 2
);

CREATE TABLE tbl_result (
	  id bigint,
	  name varchar,
	  age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern_2;

INSERT INTO tbl_result
MATCH (a) RETURN a.id, a.name, a.age
;