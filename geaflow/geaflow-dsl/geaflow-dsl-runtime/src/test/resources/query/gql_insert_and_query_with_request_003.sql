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

set geaflow.dsl.window.size = 2;

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

CREATE TABLE modern_vertex (
  id varchar,
  type varchar,
  name varchar,
  other varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/modern_vertex.txt'
);

CREATE TABLE modern_edge (
  srcId bigint,
  targetId bigint,
  type varchar,
  weight double
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/modern_edge.txt'
);

CREATE TABLE request (
  id int,
  name varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/request.txt'
);

CREATE TABLE tbl_result (
  name varchar,
  a_id bigint,
  weight double,
  label varchar,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO dy_modern.person
SELECT cast(id as bigint), name, cast(other as int) as age
FROM modern_vertex WHERE type = 'person'
;

INSERT INTO dy_modern.software
SELECT cast(id as bigint), name, other as lang
FROM modern_vertex WHERE type = 'software'
;

INSERT INTO dy_modern.knows
SELECT srcId, targetId, weight
FROM modern_edge WHERE type = 'knows'
;

INSERT INTO dy_modern.created
SELECT srcId, targetId, weight
FROM modern_edge WHERE type = 'created'
;

USE GRAPH dy_modern;

INSERT INTO tbl_result
SELECT
  name,
	a_id,
	weight,
	label,
	b_id
FROM (
  WITH p AS (
      SELECT * FROM request where name is not null
    )
  MATCH (a where id = p.id) -[e]->(b)
  RETURN p.name as name, a.id as a_id, e.weight as weight, label(e) as label, b.id as b_id
)
