CREATE GRAPH modern (
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
	geaflow.dsl.using.vertex.path = 'resource:///data/modern_vertex.txt',
	geaflow.dsl.using.edge.path = 'resource:///data/modern_edge.txt'
);


USE GRAPH modern;

MATCH (a) -[e:knows]->(b:person where b.id != 1)
RETURN a,b,e, a.id
