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
	shardCount = 1
);

CREATE TABLE tbl_result (
  vid int,
	prValue double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH dy_modern;

INSERT INTO dy_modern.person(id, name, age)
SELECT 1, 'jim', 20
UNION ALL
SELECT 2, 'kate', 22
;

INSERT INTO dy_modern.knows
SELECT 1, 2, 0.2
;

INSERT INTO dy_modern(person.id, person.name, knows.srcId, knows.targetId)
SELECT 3, 'jim', 3, 2
;

INSERT INTO tbl_result
CALL page_rank(0.85, 0.01, 20) YIELD (vid, prValue)
RETURN vid, ROUND(prValue, 3)
;