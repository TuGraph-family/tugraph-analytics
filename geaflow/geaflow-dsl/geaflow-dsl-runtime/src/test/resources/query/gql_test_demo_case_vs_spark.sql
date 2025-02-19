CREATE TABLE IF NOT EXISTS tables (
  f1 bigint,
  f2 bigint
) WITH (
  type='file',
  geaflow.dsl.window.size='2',
  geaflow.dsl.column.separator=',',
  geaflow.dsl.source.parallelism = '4',
  geaflow.dsl.file.path = 'resource:///data/gql_test_demo_case_vs_spark.txt'
  -- In the online testing experiment, we place the source file in HDFS for reading.
  -- In addition, we set the source parallelism to 32 In the online testing experiment.
  -- We set window size to 16000.
  -- Set column separator to '\t'. In com-friendster data set, a line is separated by '\t'.
  -- geaflow.dsl.file.path = 'hdfs://rayagsecurity-42-033147014062:9000/com-friendster.ungraph.txt',
  -- geaflow.dsl.source.parallelism = '32',
  -- geaflow.dsl.window.size='16000',
  -- geaflow.dsl.column.separator='\t',
);

CREATE GRAPH modern (
  Vertex v1 (
    id int ID
  ),
  Edge e1 (
    srcId int SOURCE ID,
    targetId int DESTINATION ID
  )
) WITH (
  storeType='memory',
  shardCount = 4
  -- In the online testing experiment, we set shardCount parameter tot 256.
  -- shardCount = 256
);

INSERT INTO modern(v1.id, e1.srcId, e1.targetId)
(
  SELECT f1, f1, f2
  FROM tables
);

INSERT INTO modern(v1.id)
(
  SELECT f2
  FROM tables
);

CREATE TABLE IF NOT EXISTS tbl_result (
  vid bigint,
  	component bigint
) WITH (
  -- Output result to log.
  type ='console'
);

use GRAPH modern;

INSERT INTO tbl_result
CALL inc_wcc(10) YIELD (vid, component)
RETURN vid, component
;