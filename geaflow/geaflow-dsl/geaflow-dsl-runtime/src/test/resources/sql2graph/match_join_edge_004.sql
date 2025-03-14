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

CREATE TABLE IF NOT EXISTS match_join_edge_004_result (
	s_id bigint,
	c_id bigint,
	m_id bigint,
  f_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_edge_004_result
SELECT studentId1, courseId, monitorId, friendId
FROM
(
SELECT s.id as studentId1, sc.targetId * 10 as courseId
FROM student s, selectCourse sc
WHERE s.id = sc.srcId
), (
SELECT s.id as studentId2, m.targetId * 100 as monitorId
FROM student s, hasMonitor m
WHERE s.id = m.srcId
), (
SELECT s.id as studentId3, k.targetId * 1000 as friendId
FROM student s, knows k
WHERE s.id = k.srcId
)
WHERE
studentId1 = studentId2 AND studentId2 = studentId3
ORDER BY studentId1, courseId, monitorId, friendId
;