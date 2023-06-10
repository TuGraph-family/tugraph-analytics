CREATE Table v_person (
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

CREATE graph g (
  Vertex person using v_person WITH ID(id),
  Edge knows using v_knows WITH ID(s_id, t_id), TIMESTAMP(ts)
);