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