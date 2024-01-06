Create Graph IF NOT EXISTS g (
  Vertex buyer (
    id VARCHAR ID,
    name string,
    age int
  ),
  Vertex seller (
    id VARCHAR ID,
    name string
  ),
  Edge buy (
    srcId from buyer SOURCE ID,
    targetId from seller DESTINATION ID,
    ts bigint,
    amount double
  ),
  Edge knows (
    srcId from seller SOURCE ID,
    targetId from seller DESTINATION ID,
    weight double
  )
) With ( -- 配置信息
  storeType = 'memory',
  shardCount = 2
)
;

INSERT INTO g.seller SELECT '1', 'test';