
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

CREATE TABLE edges (
	srcId bigint,
	targetId bigint,
	kind varchar,
	weight double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/modern_edge.txt'
);

INSERT INTO modern_2(person.id, person.name, person.age, knows.srcId, knows.targetId)
(
  SELECT srcId, 'test1', 1, srcId, targetId
  FROM edges
  UNION ALL
  SELECT targetId, 'test2', 2, srcId, targetId
  FROM edges
)
;
