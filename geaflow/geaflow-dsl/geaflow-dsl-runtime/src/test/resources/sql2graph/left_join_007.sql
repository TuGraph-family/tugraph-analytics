CREATE TABLE IF NOT EXISTS left_join_007_result (
	f0 bigint,
	f1 bigint,
	f2 bigint,
	f3 bigint,
	f4 bigint,
	f5 bigint,
  f6 bigint,
  f7 bigint,
  f8 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_007_result
select
  `table_30`.`col_21` as `col_21`,
  `col_31` as `col_38`,
  `col_32` as `col_39`,
  `col_33` as `col_40`,
  `col_34` as `col_41`,
  `col_35` as `col_42`,
  `col_36` as `col_43`,
  (
    case
      when `col_31` = 0 then null
      else `col_33` /`col_31`
    end
  ) as `col_44`,
  (
    case
      when `col_32` = 0 then null
      else `col_34` /`col_32`
    end
  ) as `col_45`
from
  (
    select
      `table_29`.`col_21` as `col_21`,
      sum(`col_15`) as `col_31`,
      count(
        distinct
        IF(`table_14`.`col_2` % 2 = 0, `table_14`.`col_8`, NULL)
      ) as `col_32`,
      sum(`col_16`) as `col_33`,
      count(
        distinct
        IF(`table_14`.`col_2` % 2 = 1, `table_14`.`col_8`, NULL)
      ) as `col_34`,
      sum(`col_17`) as `col_35`,
      sum(`col_18`) as `col_36`
    from
      (
        select
          `table_11`.`id` as `col_6`,
          `table_13`.`col_8` as `col_8`,
          `table_13`.`col_2` as `col_2`,
          count(
            IF(`table_13`.`col_2` % 4 = 0, `table_13`.`col_3`, NULL)
          ) as `col_15`,
          count(
          IF(`table_13`.`col_2` % 4 = 1, `table_13`.`col_3`, NULL)
          ) as `col_16`,
          sum(
          IF(`table_13`.`col_2` % 4 = 2, `table_13`.`col_4`, NULL)
          ) as `col_17`,
          sum(
          IF(`table_13`.`col_2` % 4 = 3, `table_13`.`col_4`, NULL)
          ) as `col_18`
        from
          (
            select
              `table_12`.`targetId` as `col_10`,
              `table_12`.`targetId` as `col_2`,
              `table_12`.`targetId` as `col_3`,
              CAST(`table_12`.`targetId` as DOUBLE) as `col_4`,
              `table_12`.`srcId` as `col_8`
            from
              selectCourse `table_12`
          ) `table_13`
          LEFT JOIN student `table_11` on `table_13`.`col_8` = `table_11`.`id`
        group by
          `table_11`.`id`,
          `table_13`.`col_8`,
          `table_13`.`col_2`
      ) `table_14`
      INNER JOIN (
        select
          `table_28`.`col_6` as `col_6`,
          `table_28`.`col_21` as `col_21`
        from
          (
            select
              `table_25`.`id` as `col_6`,
              `table_27`.`col_21` as `col_21`
            from
              (
                select
                  `table_26`.`targetId` as `col_24`,
                  `table_26`.`targetId` as `col_21`,
                  `table_26`.`srcId` as `col_23`
                from
                  hasMonitor `table_26`
              ) `table_27`
              LEFT JOIN student `table_25` on `table_27`.`col_23` = `table_25`.`id`
            group by
              `table_25`.`id`,
              `table_27`.`col_21`
          ) `table_28`
        group by
          `table_28`.`col_6`,
          `table_28`.`col_21`
      ) `table_29` on `table_14`.`col_6` = `table_29`.`col_6`
      and `table_14`.`col_6` = `table_29`.`col_6`
    group by
      `table_29`.`col_21`
  ) `table_30`
order by
  `col_38` DESC
limit
  10000
  ;