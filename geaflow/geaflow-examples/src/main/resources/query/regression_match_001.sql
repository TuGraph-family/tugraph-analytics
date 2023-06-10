CREATE GRAPH IF NOT EXISTS match001_graph (
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

INSERT INTO match001_graph.person
SELECT 1,'marko',29
UNION ALL
SELECT 2,'vadas',27
UNION ALL
SELECT 4,'josh',32
UNION ALL
SELECT 6,'peter',35
;

INSERT INTO match001_graph.software
SELECT 3,'lop','java'
UNION ALL
SELECT 5,'ripple','java'
;


INSERT INTO match001_graph.created
SELECT 1,3,0.4
UNION ALL
SELECT 4,3,0.4
UNION ALL
SELECT 4,5,1.0
UNION ALL
SELECT 6,3,0.2
;

INSERT INTO match001_graph.knows
SELECT 1,2,0.5
UNION ALL
SELECT 1,4,1.0
;

CREATE TABLE IF NOT EXISTS tbl_result (
  a_id bigint,
  weight double,
  b_id bigint
) WITH (
	type='file'
);

USE GRAPH match001_graph;

INSERT INTO tbl_result
SELECT
	a_id,
	weight,
	b_id
FROM (
  MATCH (a) -[e:knows]->(b:person where b.id != 1)
  RETURN a.id as a_id, e.weight as weight, b.id as b_id
)
;