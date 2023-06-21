set geaflow.dsl.window.size = 1;

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

CREATE TABLE tbl_result (
  name varchar,
  a_id bigint,
  weight double,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO dy_modern.person(id, name, age)
SELECT 1, 'jim', 20
UNION ALL
SELECT 2, 'kate', 22
;

INSERT INTO dy_modern.knows
SELECT 1, 2, 0.2
;


INSERT INTO dy_modern(person.id, person.name, knows.srcId, knows.targetId)
SELECT 3, 'jim', 3, 2
;

USE GRAPH dy_modern;

INSERT INTO tbl_result
SELECT
  name,
	a_id,
	weight,
	b_id
FROM (
  WITH p AS (
      SELECT * FROM (VALUES(1, 'r0'), (3, 'r1')) AS t(id, name)
    )
  MATCH (a where id = p.id) -[e:knows]->(b:person)
  RETURN p.name as name, a.id as a_id, e.weight as weight, b.id as b_id
)
