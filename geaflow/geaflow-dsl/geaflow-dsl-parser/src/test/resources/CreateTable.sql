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
