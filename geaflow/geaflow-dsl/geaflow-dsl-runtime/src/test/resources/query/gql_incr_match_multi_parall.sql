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

CREATE GRAPH modern (
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
geaflow.dsl.file.path = 'resource:///data/incr_modern_vertex.txt',
geaflow.dsl.window.size = 20
);

CREATE TABLE modern_edge (
  srcId bigint,
  targetId bigint,
  type varchar,
  weight double
) WITH (
  type='file',
geaflow.dsl.file.path = 'resource:///data/incr_modern_edge.txt',
geaflow.dsl.window.size = 2
);

INSERT INTO modern.person
  SELECT cast(id as bigint), name, cast(other as int) as age
  FROM modern_vertex WHERE type = 'person'
;

INSERT INTO modern.software
SELECT cast(id as bigint), name, cast(other as varchar) as lang
FROM modern_vertex WHERE type = 'software'
;

INSERT INTO modern.knows
  SELECT srcId, targetId, weight
  FROM modern_edge WHERE type = 'knows'
;

INSERT INTO modern.created
  SELECT srcId, targetId, weight
  FROM modern_edge WHERE type = 'created'
;

CREATE TABLE tbl_result (
  a_id BIGINT,
  b_id BIGINT,
  c_id BIGINT,
  d_id BIGINT
) WITH (
	type='file',
geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
  SELECT
  a_id, b_id, c_id,d_id
  FROM (
  MATCH (a:person) -[e:knows]->(b:person)<-[e2:knows]-(c:person)<-[e3:knows]-(d:person) where a.id!=c.id
  RETURN a.id as a_id,b.id as b_id,c.id as c_id , d.id as d_id
  )
;