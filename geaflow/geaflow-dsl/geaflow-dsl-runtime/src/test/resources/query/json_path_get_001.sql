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

set geaflow.dsl.column.separator = '|';
CREATE TABLE users (
	id long,
	name string,
	age double,
	json_string string
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/users_json_object.txt'
);

CREATE TABLE tbl_result (
	col1 varchar,
	col2 varchar,
	col3 varchar,
	col4 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT
    get_json_object(o.json_string, '$.name') AS col1,
    get_json_object(o.json_string, '$.age') AS col2,
    get_json_object(o.json_string, '$.address.city') AS col3,
    get_json_object(o.json_string, '$.items[2]') AS col4
FROM users o;