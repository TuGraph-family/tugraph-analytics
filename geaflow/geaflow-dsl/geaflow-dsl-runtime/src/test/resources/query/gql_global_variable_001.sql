CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  isTop Boolean,
  flag int
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
	b_id,
	isTop,
	flag
FROM (
  Match(a)
  Let Global a.isTop = if(a.id = 3, true, false)
  Match(a) ->(b) Where b.isTop = true
  Let Global b.flag = 1
  RETURN a.id AS a_id, b.id AS b_id, b.isTop as isTop, b.flag as flag
)

