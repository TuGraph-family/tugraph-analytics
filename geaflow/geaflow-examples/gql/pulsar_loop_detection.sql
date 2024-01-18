set geaflow.dsl.window.size = 1;
set geaflow.dsl.ignore.exception = true;


CREATE GRAPH IF NOT EXISTS pulsar_modern8 (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS pulsar_source (
    text varchar
) WITH (
    type='pulsar',
    `geaflow.dsl.column.separator` = '#',
    `geaflow.dsl.pulsar.servers` = 'hadoop100,hadoop102,hadoop103',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_read_partition',
    `geaflow.dsl.pulsar.subscriptionInitialPosition` = 'latest'
    );

CREATE TABLE IF NOT EXISTS pulsar_sink (
    a_id bigint,
    b_id bigint,
    c_id bigint,
    d_id bigint,
    a1_id bigint
) WITH (
    type='pulsar',
    `geaflow.dsl.pulsar.servers` = 'hadoop100,hadoop102,hadoop103',
    `geaflow.dsl.pulsar.port` = '6650',
    `geaflow.dsl.pulsar.topic` = 'persistent://test/test_pulsar_connector/topic_write'
    );

USE GRAPH pulsar_modern8;

INSERT INTO pulsar_modern8.person(id, name)
SELECT
    cast(trim(split_ex(t1, ',', 0)) as bigint),
    split_ex(trim(t1), ',', 1)
FROM (
         Select trim(substr(text, 2)) as t1
         FROM pulsar_source
         WHERE substr(text, 1, 1) = '.'
     );

INSERT INTO pulsar_modern8.knows
SELECT
    cast(split_ex(t1, ',', 0) as bigint),
    cast(split_ex(t1, ',', 1) as bigint),
    cast(split_ex(t1, ',', 2) as double)
FROM (
         Select trim(substr(text, 2)) as t1
         FROM pulsar_source
         WHERE substr(text, 1, 1) = '-'
     );

INSERT INTO pulsar_sink
SELECT
    a_id,
    b_id,
    c_id,
    d_id,
    a1_id
FROM (
      MATCH (a:person) -[:knows]->(b:person) -[:knows]-> (c:person)
          -[:knows]-> (d:person) -> (a:person)
          RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id, a.id as a1_id
     );
