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

CREATE TABLE tables (
  f1 bigint,
	f2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/input/email_edge'
);

CREATE TABLE tbl_result (
  f1 bigint,
	f2 bigint
) WITH (
	type = 'sls'
);

INSERT INTO tbl_result
select f1, f2 from tables
;