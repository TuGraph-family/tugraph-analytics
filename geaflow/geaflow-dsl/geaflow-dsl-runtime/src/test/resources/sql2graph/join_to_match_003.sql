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

CREATE TABLE IF NOT EXISTS join_to_match_003_result (
	s_id bigint,
	c_id bigint,
  c_name varchar,
  t_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_003_result
SELECT s.id, c.id, c.name, hasTeacher.targetId
FROM hasTeacher, course c, selectCourse sc, student s
WHERE c.id = sc.targetId AND c.id = hasTeacher.srcId AND s.id = sc.srcId AND sc.srcId < 1004
ORDER BY sc.srcId, sc.targetId
;