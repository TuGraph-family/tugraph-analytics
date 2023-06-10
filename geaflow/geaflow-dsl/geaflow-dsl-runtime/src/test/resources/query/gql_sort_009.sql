CREATE TABLE tbl_result (
  a_id bigint,
  a_cnt bigint,
  d_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	a_cnt,
	d.id
FROM (
  MATCH (a where id in (4))-(b)-(c)-(d)
  Let a.cnt = d.age
  MATCH (d)-(f)-(g) order by a.cnt DESC, a, d
  RETURN DISTINCT a, a.cnt as a_cnt, d
)
;


