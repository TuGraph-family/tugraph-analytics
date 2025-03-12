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

CREATE TABLE orders (
	createTime bigint,
  productId bigint,
  orderId bigint,
  units bigint,
  user_name VARCHAR
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/orders.txt'
);

CREATE TABLE output_console(
	f0 bigint,
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	f5 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

CREATE VIEW IF NOT EXISTS console (a, b, c, d, e, f) AS
SELECT
  COUNT(o.productId) AS count_id,
  SUM(o.productId) AS sum_id,
  MAX(o.productId) AS max_id,
  MIN(o.productId) AS min_id,
  COUNT(DISTINCT o.productId) AS distinct_id,
  o.user_name
FROM orders o
WHERE o.units > 10
GROUP BY o.user_name;

INSERT INTO output_console
SELECT * FROM console
where f is not null and f <> 'test' ;

