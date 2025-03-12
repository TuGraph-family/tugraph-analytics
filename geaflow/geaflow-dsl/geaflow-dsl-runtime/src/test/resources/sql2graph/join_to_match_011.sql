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

CREATE TABLE IF NOT EXISTS join_to_match_011_result (
	studentId bigint,
	selectSrc bigint,
	selectTarget bigint,
  courseId bigint,
  cnt bigint,
  teacherId bigint,
  teacherId2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_011_result
SELECT studentId, selectSrc, selectTarget, courseId, teacherId, cnt, teacher.id as teacherId2
FROM
(
SELECT s.id as studentId, sc.srcId as selectSrc, sc.targetId as selectTarget, c.id as courseId,
hasTeacher.targetId as teacherId, s.id * 100000000 + c.id * 10000 + hasTeacher.targetId as cnt
FROM student s, selectCourse sc, course c, hasTeacher
WHERE s.id = sc.srcId AND c.id = sc.targetId AND c.id = hasTeacher.srcId
) tableA, teacher
WHERE tableA.teacherId = teacher.id
ORDER BY studentId, selectSrc, selectTarget, courseId, teacherId, cnt
;