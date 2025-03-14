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

CREATE TABLE IF NOT EXISTS aggregate_to_match_002_result (
	sId bigint,
	courseNum bigint,
  teacherId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO aggregate_to_match_002_result
SELECT * FROM (
SELECT sId, teacherId, MAX(student.id) as sId2
FROM (
SELECT DISTINCT student.id as sId, teacher.id as teacherId
FROM student, selectCourse, course, hasTeacher, teacher
WHERE student.id = selectCourse.srcId AND course.id = selectCourse.targetId
  AND course.id = hasTeacher.srcId AND teacher.id = hasTeacher.targetId
), hasMonitor, student
WHERE teacherId = hasMonitor.targetId AND hasMonitor.srcId = student.id
GROUP BY sId, teacherId
)
WHERE sId = sId2
;