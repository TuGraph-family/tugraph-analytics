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
