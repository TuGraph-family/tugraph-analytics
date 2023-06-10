CREATE TABLE tbl_result (
  a_id bigint,
  weight bigint,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern_ts;

INSERT INTO tbl_result
SELECT
	a_id,
	ts,
	b_id
FROM (
Match
(a WHERE id in (1,2,3,4,5,6))-[e
WHERE (ts = 1 or ts = 2 or 3 <= ts and 3 >= ts) and (ts = 2 or ts = 3 or 6 = ts)]-(b)
RETURN a.id as a_id, e.ts as ts, b.id as b_id
)
