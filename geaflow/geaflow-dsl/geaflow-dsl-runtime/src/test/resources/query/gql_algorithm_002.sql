CREATE TABLE tbl_result (
  vid int,
	prValue double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
CALL page_rank(1) YIELD (vid, prValue)
RETURN vid, ROUND(prValue, 2)
;