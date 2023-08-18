set geaflow.dsl.window.size = 1;

CREATE GRAPH modern_distinct (
	Vertex person (
	  id bigint ID,
	  name varchar,
	  age int
	),
  Edge knows (
    srcId from person SOURCE ID,
    targetId from person DESTINATION ID,
    weight double
  )
) WITH (
	storeType='rocksdb',
	shardCount = 1
);

CREATE TABLE tbl_result (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

CREATE TABLE tbl_person (
	id bigint,
	name varchar,
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path='resource:///data/modern_vertex_person.txt'
);

USE GRAPH modern_distinct;

INSERT INTO modern_distinct.person SELECT * FROM tbl_person;

INSERT INTO tbl_result MATCH (a:person)
RETURN DISTINCT a.id, a.name, a.age
ORDER BY id
;