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

CREATE GRAPH modern_2 (
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
	shard.count = 2
);

CREATE TABLE tbl_result (
	  id bigint,
	  name varchar,
	  age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern_2;

INSERT INTO tbl_result
MATCH (a) RETURN a.id, a.name, a.age
;