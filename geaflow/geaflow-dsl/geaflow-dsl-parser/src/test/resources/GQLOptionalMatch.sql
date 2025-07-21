/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

-- 测试1 (源自大作业验收标准): 验证 "MATCH ... OPTIONAL MATCH ... RETURN" 的核心模式。
-- 查找ID为1的Person，并可选地查找他认识的人，最后返回两者的名字。
MATCH (p:person WHERE id = 1) 
OPTIONAL MATCH (p)-[r:knows]->(c:person) 
RETURN p.name AS p_name, c.name AS c_name;

-- 测试2: 基础的OPTIONAL MATCH
-- 查找用户a，并可选地查找他指向的任一用户b。
MATCH (a:user) 
OPTIONAL MATCH (a)-[e:knows]->(b:user) 
RETURN a.name as a_name, b.name as b_name;

-- 测试3: 带有属性过滤的OPTIONAL MATCH
-- 查找用户a，并可选地查找他通过权重>0.8的knows关系连接的用户b。
MATCH (a:user WHERE a.name = 'marko') 
OPTIONAL MATCH (a)-[e:knows WHERE e.weight > 0.8]->(b:user) 
RETURN a.id AS a_id, e.weight AS weight, b.id AS b_id;

-- 测试4: 带有外部WHERE子句的OPTIONAL MATCH
-- 查找用户a及其可选的电影评分，但只保留评分>4的结果。这会使OPTIONAL变为强制。
MATCH (a:user) 
OPTIONAL MATCH (a)-[r:rates]->(m:movie) 
WHERE r.score > 4 
RETURN a.name AS user_name, m.title AS movie_title, r.score as score;

-- 测试5: 链式的OPTIONAL MATCH
-- 查找作者p，可选地查找他写的书b，再可选地查找由书改编的剧集s。
MATCH (p:author WHERE p.name = 'George R. R. Martin') 
OPTIONAL MATCH (p)-[:wrote]->(b:book) 
OPTIONAL MATCH (b)<-[:adapted_from]-(s:show) 
RETURN p.name AS author_name, b.title AS book_title, s.name AS show_name;

-- 测试6: 在复杂MATCH后使用OPTIONAL MATCH
-- 查找一个共同关注b和c的用户a，然后可选地查找b和c之间是否存在朋友关系。
MATCH (a:user)-[:knows]->(b:user), (a)-[:knows]->(c:user) 
OPTIONAL MATCH (b)-[e:friends_with]->(c) 
RETURN b.id AS b_id, c.id AS c_id, e.src_id IS NOT NULL AS is_friend;

-- 测试7: OPTIONAL MATCH + 聚合函数
-- 验证 GeaFlow DSL 中 RETURN 子句的 GROUP BY 能否正常解析。
MATCH (a:user) 
OPTIONAL MATCH (a)-[:knows]->(b:user) 
RETURN a.id AS a_id, COUNT(b.id) AS b_count GROUP BY a.id;

-- 测试8: OPTIONAL MATCH 与 UNION 联合使用
-- 验证类型推导在 UNION 场景下是否兼容。
MATCH (a:user WHERE a.id = 1) 
OPTIONAL MATCH (a)-[:knows]->(b:user) 
RETURN a.name AS name1, b.name AS name2 
UNION 
MATCH (c:user WHERE c.id = 2) 
RETURN c.name AS name1, CAST(NULL AS VARCHAR) AS name2;
