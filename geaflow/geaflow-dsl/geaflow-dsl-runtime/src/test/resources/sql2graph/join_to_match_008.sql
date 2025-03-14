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

CREATE TABLE IF NOT EXISTS join_to_match_008_result (
	f1 bigint,
	f2 bigint,
  f3 bigint,
  f4 varchar,
  f5 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_008_result
SELECT studentId1, courseId1, courseId2, table1.cnt, table2.cnt
FROM
(
SELECT studentId1, courseId * 10 as courseId1, cnt FROM (
SELECT s.id as studentId1, c.id as courseId, c.cnt
FROM student s, selectCourse sc, (SELECT *, 'c1' as cnt FROM course) c, hasTeacher teach, teacher t
WHERE s.id = sc.srcId AND c.id = sc.targetId AND c.id = teach.srcId AND t.id = teach.targetId )
) table1, (
SELECT studentId2, courseId * 100 as courseId2, cnt FROM (
SELECT s.id as studentId2, c.id as courseId, c.cnt
FROM student s, selectCourse sc, (SELECT *, 'c2' as cnt FROM course) c
WHERE s.id = sc.srcId AND c.id = sc.targetId )
) table2
WHERE
studentId1 = studentId2
ORDER BY studentId1, courseId1, courseId2
;
