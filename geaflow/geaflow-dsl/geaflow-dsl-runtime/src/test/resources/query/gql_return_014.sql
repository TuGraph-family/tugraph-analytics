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
  f0 bigint,
  f1 bigint,
  f2 bigint,
  f3 bigint,
  f4 double,
  f5 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_name,
	e_weight,
	b_age_max,
	b_age_min,
	b_age_avg,
	_b.id
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  RETURN COUNT(a.name) as a_name, SUM(e.weight) as e_weight,
  MAX(b.age) as b_age_max, MIN(b.age) as b_age_min,
  AVG(b.age) as b_age_avg, b as _b
  group by _b order by _b
)
