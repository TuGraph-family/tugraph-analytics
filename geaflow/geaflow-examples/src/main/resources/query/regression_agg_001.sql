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

CREATE TABLE IF NOT EXISTS tables (
	f1 bigint,
	f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/input/email_edge'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  f1 bigint,
	f2 bigint,
	f3 bigint
) WITH (
	type ='file'
);

INSERT INTO tbl_result
select
  f1, sum(f2) as f2_sum, sum(cnt) as cnt_sum
FROM (
  select f1, f2, count(1) as cnt from tables u  group by f1, f2
) a
GROUP BY f1
ORDER BY f1, f2_sum, cnt_sum
;