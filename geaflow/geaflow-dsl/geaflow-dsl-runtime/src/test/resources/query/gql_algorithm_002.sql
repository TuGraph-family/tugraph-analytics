CREATE TABLE tbl_result (
  vid int,
	prValue double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
CALL page_rank(0.85, 0.01, 20) YIELD (vid, prValue)
RETURN vid, ROUND(prValue, 3)
;