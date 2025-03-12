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

CREATE TABLE IF NOT EXISTS left_join_005_result (
	f0 bigint,
	f1 bigint,
	f2 bigint,
  f3 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_005_result
SELECT sid, fid, table_right.sid2, fid2 from
(
SELECT student.id as sid, knows.targetId as fid from
student, knows
where student.id = knows.srcId
) table_left
LEFT JOIN
(
SELECT student.id as sid2, knows.targetId as fid2 from
student, knows
where student.id = knows.srcId
) table_right
on table_left.fid = table_right.sid2
;