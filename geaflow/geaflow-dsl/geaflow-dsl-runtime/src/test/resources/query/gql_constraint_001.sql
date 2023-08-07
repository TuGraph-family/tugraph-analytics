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
    srcId FROM buyer SOURCE ID,
    targetId FROM seller DESTINATION ID,
    ts bigint,
    amount double
  ),
  Edge knows (
    srcId FROM buyer SOURCE ID,
    targetId FROM buyer DESTINATION ID,
    weight double
  )
) With ( -- 配置信息
  storeType = 'memory',
  shardCount = 2
)
;

INSERT INTO g.seller SELECT 1, 'test';