Create Graph IF NOT EXISTS g (
  Vertex buyer (
    id bigint ID,
    name string,
    age int
  ),
  Vertex seller (
    id bigint ID,
    name string
  ),
  Edge buy (
    b_id bigint SOURCE ID,
    s_id bigint DESTINATION ID,
    ts bigint TIMESTAMP,
    amount double
  ),
  Edge knows (
    s_id bigint SOURCE ID,
    t_id bigint DESTINATION ID,
    weight double
  )
) With (
  storeType = 'cstore',
  shardCount = 1024
)
;

Create TEMPORARY Graph IF NOT EXISTS g2 (
  Vertex buyer (
    id bigint ID,
    name string,
    age int
  ),
  Vertex seller (
    id bigint ID,
    name string
  ),
  Edge buy (
    b_id bigint SOURCE ID,
    s_id bigint DESTINATION ID,
    ts bigint TIMESTAMP,
    amount double
  ),
  Edge knows (
    s_id bigint SOURCE ID,
    t_id bigint DESTINATION ID,
    weight double
  )
) With ( -- 配置信息
  storeType = 'cstore',
  shardCount = 1024
)
;