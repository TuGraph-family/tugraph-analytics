set geaflow.dsl.max.traversal=6;
set geaflow.dsl.table.parallelism=4;

CREATE GRAPH modern (
	Vertex node (
	  id int ID
	),
	Edge relation (
	  srcId int SOURCE ID,
	  targetId int DESTINATION ID
	)
) WITH (
	storeType='rocksdb',
	shardCount = 4
);

CREATE TABLE web_google_20 (
  src varchar,
  dst varchar
) WITH (
  type='file',
  geaflow.dsl.table.parallelism='4',
  geaflow.dsl.column.separator='\t',
  `geaflow.dsl.source.file.parallel.mod`='true',
  geaflow.dsl.file.path = 'resource:///data/web-google-20',
  geaflow.dsl.window.size = 8
);

INSERT INTO modern.node
SELECT cast(src as int)
FROM web_google_20
;

INSERT INTO modern.node
SELECT cast(dst as int)
FROM web_google_20
;

INSERT INTO modern.relation
SELECT cast(src as int), cast(dst as int)
FROM web_google_20;
;

CREATE TABLE tbl_result (
  ret varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
CALL inc_khop(4) YIELD (ret)
RETURN ret
;