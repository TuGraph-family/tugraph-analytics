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
	shardCount = 2
);

CREATE TABLE tbl_result (
	  f1 bigint,
	  f2 bigint,
	  f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern_2;

INSERT INTO tbl_result
MATCH (a:person)->(b)<-(c)
WHERE a.id <> c.id
RETURN a.id as f1, b.id as f2, c.id as f3
order by f1, f2, f3
;