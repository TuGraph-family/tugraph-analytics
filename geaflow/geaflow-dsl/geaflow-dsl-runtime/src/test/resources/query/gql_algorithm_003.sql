set geaflow.dsl.window.size = -1;

CREATE GRAPH friend (
	Vertex person (
  	  id bigint ID,
  	  name varchar,
  	  age int
  	),
  	Edge knows (
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

CREATE TABLE modern_vertex (
  id varchar,
  type varchar,
  name varchar,
  other varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/modern_vertex.txt'
);

CREATE TABLE modern_edge (
  srcId bigint,
  targetId bigint,
  type varchar,
  weight double
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/modern_edge.txt'
);

INSERT INTO friend.person
SELECT cast(id as bigint), name, cast(other as int) as age
FROM modern_vertex WHERE type = 'person'
;

INSERT INTO friend.knows
SELECT srcId, targetId, weight
FROM modern_edge WHERE type = 'knows'
;

USE GRAPH friend;

INSERT INTO tbl_result
CALL SSSP(1) YIELD (vid, distance)
RETURN cast (vid as int), distance
;