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

CREATE TABLE IF NOT EXISTS v_person (
  id bigint,
  name varchar,
  age int
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_person.txt'
);

CREATE TABLE IF NOT EXISTS v_software (
  id bigint,
  name varchar,
  lang varchar
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_software.txt'
);

CREATE TABLE IF NOT EXISTS e_knows (
  srcId bigint,
  targetId bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_edge_knows.txt'
);


CREATE TABLE IF NOT EXISTS e_created (
  srcId bigint,
  targetId bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_edge_created.txt'
);

CREATE GRAPH IF NOT EXISTS using_modern (
	Vertex person using v_person WITH ID(id),
	Vertex software using v_software WITH ID(id),
	Edge knows using e_knows WITH ID(srcId, targetId),
	Edge created using e_created WITH ID(srcId, targetId)
) WITH (
	storeType='rocksdb',
	shardCount = 2
);
