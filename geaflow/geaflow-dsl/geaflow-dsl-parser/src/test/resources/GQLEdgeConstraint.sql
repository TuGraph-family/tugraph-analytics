Create Graph IF NOT EXISTS g (
  Vertex buyer (
    id bigint ID,
    name string,
    age int
  ),
  Vertex seller (
    id varchar ID,
    name string
  ),
  Edge buy (
    b_id from buyer SOURCE ID,
    s_id from seller DESTINATION ID,
    ts bigint TIMESTAMP,
    amount double
  ),
  Edge knows (
    s_id from buyer SOURCE ID,
    t_id from buyer DESTINATION ID,
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
    b_id from buyer SOURCE ID,
    s_id from seller DESTINATION ID,
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
