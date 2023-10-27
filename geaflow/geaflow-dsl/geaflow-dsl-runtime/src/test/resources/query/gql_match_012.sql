CREATE TABLE v_person (
  name varchar,
  age int,
  personId bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_person_reorder.txt'
);

CREATE TABLE v_software (
  name varchar,
  lang varchar,
  softwareId bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_software_reorder.txt'
);

CREATE TABLE e_knows (
  knowsSrc bigint,
  knowsTarget bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_edge_knows.txt'
);

CREATE TABLE e_created (
  createdSource bigint,
  createdTarget bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_edge_created.txt'
);

CREATE GRAPH modern (
	Vertex person using v_person WITH ID(personId),
	Vertex software using v_software WITH ID(softwareId),
	Edge knows using e_knows WITH ID(knowsSrc, knowsTarget),
	Edge created using e_created WITH ID(createdSource, createdTarget)
) WITH (
	storeType='memory',
	shardCount = 2
);

CREATE TABLE tbl_result (
  a_id bigint,
  a_id2 bigint,
  srcId bigint,
  targetId bigint,
  b_id bigint,
  b_id2 bigint,
  vLabel varchar,
  eLabel varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
  a_id2,
  srcId,
  targetId,
  b_id,
  b_id2,
  vLabel,
  eLabel
FROM (
  MATCH (a:person) <-[e]-(b) |+| (a:software) <-[e]-(b)
  RETURN id(a) as a_id, a.~id as a_id2,
  srcId(e) as srcId, targetId(e) as targetId,
  id(b) as b_id, b.~id as b_id2,
  label(a) as vLabel, label(e) as eLabel
);