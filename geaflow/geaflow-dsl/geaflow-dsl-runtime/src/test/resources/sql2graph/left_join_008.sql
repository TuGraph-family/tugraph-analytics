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

CREATE TABLE IF NOT EXISTS left_join_008_result (
	f0 bigint,
	f1 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_008_result
select
    `col_15` as `col_15`,
    count(distinct `table_17`.`col_3`) as `col_19`
from
    (
        select
            `table_16`.`age` as `col_0`,
            `table_16`.`age` as `col_7`,
            `table_16`.`id` as `col_3`
        from
            student `table_16`
        where
            `table_16`.`id` != 0
            and (`table_16`.`age` >= 0)
    ) `table_17`
    LEFT JOIN (
        SELECT
            `col_3` as `id`,
            `col_1` as `dt`,
            `col_15` as `col_15`
        FROM
            (
                select
                    `table_13`.`srcId` as `col_3`,
                    `table_13`.`targetId` as `col_1`,
                    sum(CAST(`table_13`.`targetId` as DOUBLE)) as `col_15`
                from
                    selectCourse `table_13`
                where
                    (`table_13`.`targetId` >= 0)
                group by
                    `table_13`.`srcId`,
                    `table_13`.`targetId`
            ) AS `tableview_11`
    ) `table_11` on `table_17`.`col_3` = `table_11`.`id`
group by
    `col_15`
limit
    10000
;