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