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

CREATE TABLE IF NOT EXISTS v_student (
  id bigint,
  name varchar,
  age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/student.txt'
);

CREATE TABLE IF NOT EXISTS v_course (
  id bigint,
  name varchar,
  course_hour int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/course.txt'
);

CREATE TABLE IF NOT EXISTS v_gradeClass (
  id bigint,
  grade bigint,
  classNumber bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/gradeClass.txt'
);

CREATE TABLE IF NOT EXISTS v_teacher (
  id bigint,
  name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/teacher.txt'
);

CREATE TABLE IF NOT EXISTS e_selectCourse (
  srcId bigint,
  targetId bigint,
  ts   bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/selectCourse.txt'
);

CREATE TABLE IF NOT EXISTS e_hasTeacher (
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/hasTeacher.txt'
);

CREATE TABLE IF NOT EXISTS e_hasMonitor (
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/hasMonitor.txt'
);

CREATE TABLE IF NOT EXISTS e_knows (
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/knows.txt'
);

CREATE GRAPH IF NOT EXISTS g_student (
  Vertex student (
    id bigint ID,
    name varchar,
    age int
  ),
  Vertex course (
    id bigint ID,
    name varchar,
    course_hour int
  ),
  Vertex teacher (
    id bigint ID,
    name varchar
  ),
  Vertex gradeClass (
    id bigint ID,
    grade bigint,
    classNumber bigint
  ),
  Edge selectCourse (
    srcId from student SOURCE ID,
    targetId from course DESTINATION ID,
    ts  bigint
  ),
  Edge hasMonitor (
    srcId from student SOURCE ID,
    targetId from teacher DESTINATION ID
  ),
  Edge knows (
    srcId from student SOURCE ID,
    targetId from student DESTINATION ID
  ),
  Edge inClass (
    srcId from student SOURCE ID,
    targetId from gradeClass DESTINATION ID
  ),
  Edge hasTeacher (
    srcId from course SOURCE ID,
    targetId from teacher DESTINATION ID
  )
) WITH (
	storeType='rocksdb',
	shardCount = 2
);



INSERT INTO g_student.student SELECT * FROM v_student;
INSERT INTO g_student.course SELECT * FROM v_course;
INSERT INTO g_student.teacher SELECT * FROM v_teacher;
INSERT INTO g_student.gradeClass SELECT * FROM v_gradeClass;
