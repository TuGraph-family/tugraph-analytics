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

/*
 * OPTIONAL MATCH 运行时集成测试 (最终单体版)
 * 结构完全参照 gql_match_007.sql 成功范例。
 */

-- -- 1. 定义数据源 (Source Tables)
-- CREATE TABLE user_source (
--   id bigint,
--   name varchar,
--   age int
-- ) WITH (
--   type='file',
--   geaflow.dsl.file.path = 'resource:///data/users_optionalmatch.txt'
-- );

-- CREATE TABLE knows_source (
--   srcId bigint,
--   targetId bigint
-- ) WITH (
--   type='file',
--   geaflow.dsl.file.path = 'resource:///data/knows_optionalmatch.txt'
-- );

-- -- 2. 定义图的 Schema 并直接绑定数据源
-- CREATE GRAPH g1 (
--   Vertex user USING user_source WITH ID(id),
--   Edge knows USING knows_source WITH ID(srcId, targetId)
-- ) WITH (
--   storeType='memory'
-- );

-- -- 3. 定义结果输出目标 (Sink)
-- CREATE TABLE tbl_result (
--   user_name varchar,
--   friend_name varchar
-- ) WITH (
--   type='file',
--   geaflow.dsl.file.path = '${target}'
-- );

-- -- 4. 设置当前查询的图上下文
-- USE GRAPH g1;

-- -- 5. 执行 OPTIONAL MATCH 查询并插入结果
-- INSERT INTO tbl_result
-- MATCH (a:user)
-- OPTIONAL MATCH (a)-[:knows]->(b:user)
-- RETURN a.name AS a_name, b.name AS b_name
-- ORDER BY a_name, b_name;

-- 1. 定义数据源 (Source Tables) 
CREATE TABLE user_source (
  id bigint,
  name varchar,
  age int
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/users_optionalmatch.txt'
);

CREATE TABLE knows_source (
  srcId bigint,
  targetId bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/knows_optionalmatch.txt'
);

-- 2. 定义图的 Schema
CREATE GRAPH g1 (
  Vertex user USING user_source WITH ID(id),
  Edge knows USING knows_source WITH ID(srcId, targetId)
) WITH (
  storeType='memory'
);

-- 3. 定义结果输出目标 (Sink)
CREATE TABLE tbl_result (
  user_name varchar,
  friend_name varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = '${target}'
);

-- 4. 设置当前查询的图上下文
USE GRAPH g1;

-- OPTIONAL MATCH
INSERT INTO tbl_result
MATCH (a:user)
OPTIONAL MATCH (a)-[:knows]->(b:user)
RETURN a.name AS a_name, b.name AS b_name
ORDER BY a_name, b_name;
