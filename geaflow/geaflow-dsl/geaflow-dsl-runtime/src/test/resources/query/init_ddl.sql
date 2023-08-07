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
	storeType='memory',
	geaflow.dsl.using.vertex.path = 'resource:///data/modern_vertex.txt',
	geaflow.dsl.using.edge.path = 'resource:///data/modern_edge.txt'
);

CREATE GRAPH modern_ts (
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
	  ts bigint TIMESTAMP
	),
	Edge created (
	  srcId bigint SOURCE ID,
  	targetId bigint DESTINATION ID,
  	ts bigint TIMESTAMP
	)
) WITH (
	storeType='memory',
	geaflow.dsl.using.vertex.path = 'resource:///data/modern_vertex.txt',
	geaflow.dsl.using.edge.path = 'resource:///data/modern_edge_ts.txt'
);


CREATE GRAPH dy_modern (
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