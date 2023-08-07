set geaflow.dsl.window.size = 1;

CREATE TABLE IF NOT EXISTS v_person (
  name varchar,
  age int,
  id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_person_reorder.txt'
);

CREATE TABLE IF NOT EXISTS v_software (
  name varchar,
  lang varchar,
  id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_software_reorder.txt'
);

CREATE TABLE IF NOT EXISTS e_knows (
  srcId bigint,
  targetId bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/modern_edge_knows.txt'
);


CREATE TABLE IF NOT EXISTS e_created (
  srcId bigint,
  targetId bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/modern_edge_created.txt'
);

CREATE TEMPORARY GRAPH using_modern_2 (
	Vertex person using v_person WITH ID(id),
	Vertex software using v_software WITH ID(id),
	Edge knows using e_knows WITH ID(srcId, targetId),
	Edge created using e_created WITH ID(srcId, targetId)
) WITH (
	storeType='rocksdb',
	shardCount = 2
);


CREATE TABLE IF NOT EXISTS tbl_result (
  a_id bigint,
  weight double,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH using_modern_2;

INSERT INTO tbl_result
SELECT
	a_id,
	e.weight,
	b_id
FROM (
  match(a:person where a.id = 1)-[e:knows]->(b:person)
  RETURN a.id AS a_id, e, b.id AS b_id
  THEN FILTER b_id = 4 AND e.weight > 0
)
;
