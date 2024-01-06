CREATE TABLE IF NOT EXISTS aggregate_to_match_003_result (
	f0 bigint,
	f1 bigint,
  f2 bigint,
  f3 bigint,
  f4 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO aggregate_to_match_003_result
select
  table_26.col_4 as col_4,
  col_27 as col_32,
  col_28 as col_33,
  col_29 as col_34,
  col_30 as col_35
from
  (
    select
      table_13.col_6 as col_4,
      sum(col_14) as col_27,
      count(
        distinct IF(table_13.col_2 % 2 = 0, table_13.col_3, cast(null as bigint))
      ) as col_28,
      sum(col_15) as col_29,
      count(
        distinct IF(table_13.col_2 % 2 = 1, table_13.col_3, cast(null as bigint))
      ) as col_30
    from
      (
        select
          table_12.col_4 as col_4,
          table_10.id as col_6,
          table_12.col_2 as col_2,
          table_12.col_3 as col_3,
          count(
            IF(table_12.col_2 % 2 = 0, table_12.col_3, cast(null as bigint))
          ) as col_14,
          count(
            IF(table_12.col_2 % 2 = 1, table_12.col_3, cast(null as bigint))
          ) as col_15
        from
          (
            select srcId as col_2, targetId as col_3, ts as col_4
            from selectCourse table11
          ) table_12, student table_10 where table_12.col_2 = table_10.id
        group by
          table_12.col_4,
          table_10.id,
          table_12.col_3,
          table_12.col_2
      ) table_13
      INNER JOIN (
        select
          table_24.col_6 as col_6
        from
          (
            select
              table_21.id as col_6
            from
              (
                select
                  table_22.srcId as col_19
                from
                  hasMonitor table_22
              ) table_23
              INNER JOIN student table_21 on table_23.col_19 = table_21.id
            group by
              table_21.id
          ) table_24
        group by
          table_24.col_6
      ) table_25 on table_13.col_6 = table_25.col_6
      and table_13.col_6 = table_25.col_6
    group by
      table_13.col_6
  ) table_26
order by
  col_32 DESC,
  col_4 DESC
limit
  10000
