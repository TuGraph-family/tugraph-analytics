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

CREATE TABLE IF NOT EXISTS match_join_edge_003_result (
	s_id bigint,
	c_id bigint,
  m_id bigint,
  k_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_edge_003_result
SELECT s.id, sc.targetId as sc_target, m.targetId as m_target, k.targetId as k_target
FROM student s, selectCourse sc, hasMonitor m, knows k
WHERE s.id = sc.srcId AND s.id = m.srcId AND s.id = k.srcId AND sc.srcId < 1004
ORDER BY s.id, sc_target, m_target, k_target
;
