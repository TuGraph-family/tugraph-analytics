set geaflow.dsl.window.size = 3;

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
	storeType='memory',
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

USE GRAPH dy_modern;

INSERT INTO dy_modern.person(id, name, age)
SELECT * FROM tbl_person;
;

INSERT INTO tbl_result
MATCH (a:person)
RETURN a.id, a.name, a.age
;
