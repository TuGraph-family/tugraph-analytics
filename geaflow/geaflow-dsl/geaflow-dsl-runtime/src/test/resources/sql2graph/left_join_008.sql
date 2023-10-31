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