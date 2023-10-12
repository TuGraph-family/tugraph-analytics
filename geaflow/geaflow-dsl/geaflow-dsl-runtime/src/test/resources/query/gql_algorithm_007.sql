set geaflow.dsl.window.size = -1;
set geaflow.dsl.ignore.exception = true;

CREATE GRAPH IF NOT EXISTS g4 (
  Vertex v4 (
    vid varchar ID,
    vvalue int
  ),
  Edge e4 (
    srcId varchar SOURCE ID,
    targetId varchar DESTINATION ID
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS v_source (
    v_id varchar,
    v_value int,
    ts varchar,
    type varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///input/test_vertex'
);

CREATE TABLE IF NOT EXISTS e_source (
    src_id varchar,
    dst_id varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///input/test_edge'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  v_id varchar,
  k_value int
) WITH (
  type='file',
   geaflow.dsl.file.path = '${target}'
);

USE GRAPH g4;

INSERT INTO g4.v4(vid, vvalue)
SELECT
v_id, v_value
FROM v_source;

INSERT INTO g4.e4(srcId, targetId)
SELECT
 src_id, dst_id
FROM e_source;

CREATE Function khop AS 'com.antgroup.geaflow.dsl.udf.graph.KHop';

INSERT INTO tbl_result(v_id, k_value)
CALL khop("1",2) YIELD (vid, kValue)
RETURN vid, kValue
;