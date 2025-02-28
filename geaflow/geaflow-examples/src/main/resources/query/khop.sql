set `geaflow.dsl.table.parallelism`={$param.key1};
set `geaflow.system.offset.backend.type`='MEMORY';
set `geaflow.dsl.file.line.split.size` = '1';
set `geaflow.dsl.source.enable.upload.metrics` = 'false';
set `geaflow.dsl.sink.enable.skip` = 'true';

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
	shardCount = {$param.key1}
);

CREATE TABLE web_google_20 (
    src varchar,
    dst varchar
) WITH (
    `type`='file',
    `geaflow.dsl.file.single.mod.read`='true',
    `geaflow.dsl.table.parallelism`='{$basePath}',
    `geaflow.dsl.column.separator`='\t',
    `geaflow.dsl.source.file.parallel.mod`='true',
    `geaflow.dsl.file.path` = 'hdfs://rayagcloudcompute-49-011148024058:9000/web-Google.txt',
    `fs.defaultFS`='hdfs://rayagcloudcompute-49-011148024058:9000'
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
      geaflow.dsl.file.path='/tmp/data/result'
);

USE GRAPH modern;

INSERT INTO tbl_result
    CALL inc_khop({$param.key2}) YIELD (ret)
RETURN ret
;