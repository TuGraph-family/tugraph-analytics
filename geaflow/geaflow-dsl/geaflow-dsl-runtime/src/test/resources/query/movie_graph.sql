CREATE TABLE v_person (
  id bigint,
  name varchar,
  born bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/movie_graph_vertex_person'
);

CREATE TABLE v_movie (
  id bigint,
  title varchar
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/movie_graph_vertex_movie'
);

CREATE TABLE e_know (
  srcId bigint,
  targetId bigint,
  weight bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/movie_graph_edge_know'
);

CREATE TABLE e_acted_in (
  srcId bigint,
  targetId bigint,
  role varchar,
  act_year bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/movie_graph_edge_act'
);

CREATE GRAPH movie_graph (
	Vertex person using v_person WITH ID(id),
	Vertex movie using v_movie WITH ID(id),
	Edge know using e_know WITH ID(srcId, targetId),
	Edge acted_in using e_acted_in WITH ID(srcId, targetId)
) WITH (
	storeType='memory',
	shardCount = 1
);
