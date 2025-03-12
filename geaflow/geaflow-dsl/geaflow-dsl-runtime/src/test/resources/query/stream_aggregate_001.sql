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
	age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_duplication.txt',
	geaflow.dsl.window.size = 1
);

CREATE TABLE tbl_result (
  max_id long,
	max_name string,
	max_age int,
  min_id long,
  min_name string,
  min_age int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
select
  max(id), max(name), max(age), min(id), min(name), min(age)
FROM users
;