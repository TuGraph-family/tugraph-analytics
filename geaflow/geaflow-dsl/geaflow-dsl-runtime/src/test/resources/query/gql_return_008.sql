CREATE TABLE tbl_result (
  b_id bigint,
  amt double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	b_id,
	amt
FROM (
  MATCH (a:person WHERE a.id = 4)-()-(b)-[e]-(c)
  RETURN b.id as b_id, SUM(e.weight * 10) as amt group by b_id, e.srcId, e.targetId
  order by amt DESC, b_id DESC
  LIMIT 5
)
