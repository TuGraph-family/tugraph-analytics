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

CREATE TABLE tbl_result (
  a_id bigint,
  a_weight double,
  b_id bigint,
  b_weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	a_weight,
	b_id,
	b_weight
FROM (
  MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
  LET a.weight = a.age / cast(100.0 as double),
  LET b.weight = b.age / cast(100.0 as double)

  RETURN a.id as a_id, a.weight as a_weight, b.id as b_id,b.weight as b_weight
)