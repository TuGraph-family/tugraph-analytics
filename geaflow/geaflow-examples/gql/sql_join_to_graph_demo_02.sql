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

set geaflow.dsl.window.size = -1;

CREATE GRAPH IF NOT EXISTS dy_modern (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS tbl_source (
  text varchar
) WITH (
  type='file',
  `geaflow.dsl.file.path` = 'resource:///demo/demo_job_data.txt',
  `geaflow.dsl.column.separator`='|'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  a_name varchar,
  b_name bigint
) WITH (
  type='file',
  `geaflow.dsl.file.path` = '/tmp/geaflow/sql_join_to_graph_demo_02_result'
);

USE GRAPH dy_modern;

INSERT INTO dy_modern.person(id, name)
  SELECT
  cast(trim(split_ex(t1, ',', 0)) as bigint),
  split_ex(trim(t1), ',', 1)
  FROM (
    Select trim(substr(text, 2)) as t1
    FROM tbl_source
    WHERE substr(text, 1, 1) = '.'
  );

INSERT INTO dy_modern.knows
  SELECT
  cast(split_ex(t1, ',', 0) as bigint),
  cast(split_ex(t1, ',', 1) as bigint),
  cast(split_ex(t1, ',', 2) as double)
  FROM (
    Select trim(substr(text, 2)) as t1
    FROM tbl_source
    WHERE substr(text, 1, 1) = '-'
  );

INSERT    INTO tbl_result
SELECT    u.name,
          friend_num
FROM      person u,
          (
          SELECT    srcId AS pid,
                    COUNT(DISTINCT targetId) AS friend_num
          FROM      knows
          GROUP BY  srcId
          ) t_friend_num
WHERE     u.id = pid;