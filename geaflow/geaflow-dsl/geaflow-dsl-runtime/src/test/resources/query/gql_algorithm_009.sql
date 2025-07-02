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

-- 设置 GeaFlow 环境参数
set geaflow.dsl.window.size = -1;
-- set geaflow.dsl.ignore.exception = true;
-- PathCycleDetection 的最大迭代次数应至少为 maxPathLength
set geaflow.max.iterator.num = 5; 

-- 定义图 g_cycle_test
CREATE GRAPH IF NOT EXISTS g_cycle_test (
  Vertex v_node (
    vid varchar ID,
    vvalue int
  ),
  Edge e_link (
    srcId varchar SOURCE ID,
    targetId varchar DESTINATION ID
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

-- 定义顶点源表
CREATE TABLE IF NOT EXISTS v_source_cycle (
    v_id varchar,
    v_value int,
    ts varchar,
    type varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///input/test_vertex' -- 指向测试顶点数据
);

-- 定义边源表
CREATE TABLE IF NOT EXISTS e_source_cycle (
    src_id varchar,
    dst_id varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///input/test_edge' -- 指向测试边数据
);

-- 定义结果输出表
CREATE TABLE IF NOT EXISTS tbl_path_cycle_results (
  cycle_path_str varchar, -- 存储环路路径的字符串表示
  path_length int         -- 存储环路的长度
) WITH (
  type='file',
  geaflow.dsl.file.path = '${target}' -- 指向测试框架中定义的输出路径
);

-- 使用图
USE GRAPH g_cycle_test;

-- 加载顶点数据
INSERT INTO g_cycle_test.v_node(vid, vvalue)
SELECT
  v_id, v_value
FROM v_source_cycle;

-- 加载边数据
INSERT INTO g_cycle_test.e_link(srcId, targetId)
SELECT
  src_id, dst_id
FROM e_source_cycle;

-- 执行 PathCycleDetection 算法
-- 参数: startVertexId (起始点), minPathLength (最小长度), maxPathLength (最大长度)
-- 查找从顶点 '1' 开始，长度在 3 到 4 之间的环路
INSERT INTO tbl_path_cycle_results(cycle_path_str, path_length)
CALL path_cycle_detection('1', 3, 4) YIELD (cycle_path, length)
RETURN cycle_path, length
ORDER BY cycle_path;