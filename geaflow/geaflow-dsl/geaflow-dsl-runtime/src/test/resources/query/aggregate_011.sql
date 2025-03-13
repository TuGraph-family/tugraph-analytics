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

create function mycount as 'com.antgroup.geaflow.dsl.runtime.query.udf.MyCount';
create function concat as 'com.antgroup.geaflow.dsl.udf.table.string.Concat';

CREATE TABLE users (
	id long,
	name string,
	age double
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_double.txt'
);

CREATE TABLE tbl_result (
	user_name varchar,
	count_id bigint,
	sum_id bigint,
	max_id bigint,
	min_id bigint,
	avg_id DOUBLE,
	distinct_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
  concat(o.name, '-' , concat(o.name, '-', o.name)) as _name,
  mycount(o.name, o.id) AS count_id,
  SUM(o.id) AS sum_id,
  MAX(o.id) AS max_id,
  MIN(o.id) AS min_id,
  AVG(o.id) AS avg_id,
  COUNT(DISTINCT o.id) AS distinct_id
FROM users o
GROUP BY _name;
