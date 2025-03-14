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

CREATE Table IF NOT EXISTS v_person (
   id bigint,
   name string,
   age int
) WITH (
  test1 = 1,
  test2 = 2
);

CREATE Table v_knows (
  s_id bigint,
  t_id bigint,
  weight double,
  ts bigint
);

CREATE Table table_partition_1 (
  s_id bigint,
  t_id bigint,
  weight double,
  dt string
) Partitioned By (dt)
;

CREATE Table table_partition_2 (
  s_id bigint,
  t_id bigint,
  weight double,
  dt string,
  hh string
)
PARTITIONED BY (dt,hh)
WITH (
  k1 = 'v1',
  k2 = 'v2'
)
;


CREATE TEMPORARY Table IF NOT EXISTS v_person (
   id bigint,
   name string,
   age int
) WITH (
  test1 = 1,
  test2 = 2
);


CREATE TEMPORARY Table table_partition_2 (
  s_id bigint,
  t_id bigint,
  weight double,
  dt string,
  hh string
)
PARTITIONED BY (dt,hh)
WITH (
  k1 = 'v1',
  k2 = 'v2'
)
;
