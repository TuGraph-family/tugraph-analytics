CREATE GRAPH IF NOT EXISTS dy_modern (
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
  a_id bigint,
  weight double,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO dy_modern.person
SELECT 1,'marko',29
UNION ALL
SELECT 2,'vadas',27
UNION ALL
SELECT 4,'josh',32
UNION ALL
SELECT 6,'peter',35
;

INSERT INTO dy_modern.software
SELECT 3,'lop','java'
UNION ALL
SELECT 5,'ripple','java'
;


INSERT INTO dy_modern.created
SELECT 1,3,0.4
UNION ALL
SELECT 4,3,0.4
UNION ALL
SELECT 4,5,1.0
UNION ALL
SELECT 6,3,0.2
;

INSERT INTO dy_modern.knows
SELECT 1,2,0.5
UNION ALL
SELECT 1,4,1.0
;

USE GRAPH dy_modern;

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
