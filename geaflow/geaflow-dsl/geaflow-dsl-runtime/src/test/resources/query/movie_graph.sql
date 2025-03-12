/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
