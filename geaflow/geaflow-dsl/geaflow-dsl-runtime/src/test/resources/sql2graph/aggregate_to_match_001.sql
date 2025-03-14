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

CREATE TABLE IF NOT EXISTS aggregate_to_match_001_result (
	sId bigint,
	sName varchar,
	courseNum bigint,
  teacherId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO aggregate_to_match_001_result
SELECT sId, sName, courseNum, teacher.id as teacherId
FROM (
SELECT student.id as sId, student.name as sName, student.age, COUNT(DISTINCT course.id) as courseNum
FROM student, selectCourse, course
WHERE student.id = selectCourse.srcId AND course.id = selectCourse.targetId
GROUP BY student.id, student.name, student.age
), hasMonitor, teacher
WHERE sId = hasMonitor.srcId AND teacher.id =  hasMonitor.targetId
ORDER BY courseNum DESC, sId
;