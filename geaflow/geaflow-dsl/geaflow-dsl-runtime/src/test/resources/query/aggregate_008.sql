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

CREATE TABLE users (
	id long,
	name string,
	age long
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_duplication.txt'
);

CREATE TABLE tbl_result (
  f0 varchar,
  f1 bigint,
  f2 bigint,
  f3 bigint,
  f4 bigint,
  f5 double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
    name,
    count(distinct id),
    count(distinct id),
    count(DISTINCT case when id%2 =0 then id else null end),
    count(DISTINCT case when id%2 =1 then id else null end),
    sum(distinct age)
from users
group by name
;
