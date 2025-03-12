SELECT name, b_id FROM (
  SELECT name, b_id FROM test_table
)
;

SELECT name, b_id FROM (
  MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
	RETURN a.name as name, b.id as b_id
)
;

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

-- from match-return
SELECT name, b_id FROM (
    MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
  	RETURN a, e, b
)
;

SELECT name, b_id FROM (
  MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
	RETURN a.name as name, b.id as b_id
)
UNION ALL
SELECT name, b_id FROM (
  MATCH (a:person WHERE a.id = '2')-[e:knows]->(b:person)
	RETURN a.name as name, b.id as b_id
)
;

SELECT id, cnt
FROM (
  MATCH (a:person WHERE a.id = p.id)-[e:knows]->(b:person where b.name = p.name)
  RETURN a.id as id, count(b.id) as cnt GROUP BY a.id
)
;

SELECT id
FROM (
  MATCH (a:person WHERE a.id = p.id)-[e:knows]->(b:person where b.name = p.name)
  RETURN n.id as id
)
;

CREATE VIEW view_1 (id) AS
SELECT id
FROM (
  MATCH (a:person WHERE a.id = p.id)-[e:knows]->(b:person where b.name = p.name)
  RETURN n.id as id
)
;
